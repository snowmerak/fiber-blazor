package ledis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StreamEntry struct {
	ID     string
	Fields []string // Key, Value, Key, Value...
}

type Stream struct {
	mu      sync.RWMutex
	Entries []StreamEntry
	lastID  string
}

func NewStream() *Stream {
	return &Stream{
		Entries: make([]StreamEntry, 0),
		lastID:  "0-0",
	}
}

// Helper to get or create Stream
func (d *DistributedMap) getOrCreateStream(key string) (*Stream, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		s := NewStream()
		val, loaded := shard.LoadOrStore(key, Item{Value: s, ExpiresAt: 0})
		if loaded {
			item := val.(Item)
			if sVal, ok := item.Value.(*Stream); ok {
				return sVal, nil
			}
			return nil, ErrWrongType
		}
		return s, nil
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		s := NewStream()
		shard.Store(key, Item{Value: s, ExpiresAt: 0})
		return s, nil
	}

	s, ok := item.Value.(*Stream)
	if !ok {
		return nil, ErrWrongType
	}
	return s, nil
}

// Helper to get Stream if exists
func (d *DistributedMap) getStream(key string) (*Stream, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return nil, nil
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		d.Del(key)
		return nil, nil
	}

	s, ok := item.Value.(*Stream)
	if !ok {
		return nil, ErrWrongType
	}
	return s, nil
}

// parseID parses "123-456" into (123, 456).
func parseID(id string) (uint64, uint64, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid ID format")
	}
	ts, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return ts, seq, nil
}

// compareIDs returns -1 if id1 < id2, 0 if equal, 1 if id1 > id2
func compareIDs(id1, id2 string) int {
	// Assumes valid IDs
	t1, s1, _ := parseID(id1)
	t2, s2, _ := parseID(id2)

	if t1 < t2 {
		return -1
	}
	if t1 > t2 {
		return 1
	}
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}
	return 0
}

// generateID creates a new ID based on * or partial ID
func (s *Stream) generateID(id string) (string, error) {
	if id == "*" {
		ts := uint64(time.Now().UnixMilli())
		lastTs, lastSeq, _ := parseID(s.lastID)

		seq := uint64(0)
		if ts < lastTs {
			// Clock skew? Redis allows this if sequence increments?
			// But for auto-ID, we usually use max(lastTs, now)
			// Redis logic: if timestamp is same as last, inc seq.
			// If timestamp > last, seq = 0.
			// Getting strictly larger ID is the requirement.
			ts = lastTs // Force forward?
			// Actually strict monotonic check happens later.
		}

		if ts == lastTs {
			seq = lastSeq + 1
		}

		newID := fmt.Sprintf("%d-%d", ts, seq)
		return newID, nil
	}

	// Manual ID or partial?
	// Handle fully manual for now.
	return id, nil
}

// XAdd appends a new entry to the stream.
// id: "*" for auto-generate.
func (d *DistributedMap) XAdd(key string, id string, fields ...string) (string, error) {
	if len(fields)%2 != 0 {
		return "", errors.New("wrong number of arguments for XADD")
	}

	s, err := d.getOrCreateStream(key)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	newID, err := s.generateID(id)
	if err != nil {
		return "", err
	}

	// Validate strictly greater
	if s.lastID != "0-0" {
		if compareIDs(newID, s.lastID) <= 0 {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	} else if newID == "0-0" {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	entry := StreamEntry{
		ID:     newID,
		Fields: fields,
	}

	s.Entries = append(s.Entries, entry)
	s.lastID = newID

	return newID, nil
}

// XLen returns the number of entries in the stream.
func (d *DistributedMap) XLen(key string) (int64, error) {
	s, err := d.getStream(key)
	if err != nil {
		return 0, err
	}
	if s == nil {
		return 0, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return int64(len(s.Entries)), nil
}

// XRange returns entries within a range [start, end].
func (d *DistributedMap) XRange(key, start, end string) ([]StreamEntry, error) {
	s, err := d.getOrCreateStream(key)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return []StreamEntry{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if start == "-" {
		start = "0-0"
	}
	if end == "+" {
		end = "18446744073709551615-18446744073709551615"
	}

	res := make([]StreamEntry, 0)
	for _, entry := range s.Entries {
		if start != "-" && compareIDs(entry.ID, start) < 0 {
			continue
		}
		if end != "+" && compareIDs(entry.ID, end) > 0 {
			break
		}
		res = append(res, entry)
	}
	return res, nil
}

// XRevRange returns entries in reverse order.
func (d *DistributedMap) XRevRange(key, end, start string) ([]StreamEntry, error) {
	s, err := d.getOrCreateStream(key)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return []StreamEntry{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if start == "-" {
		start = "0-0"
	}
	if end == "+" {
		end = "18446744073709551615-18446744073709551615"
	}

	res := make([]StreamEntry, 0)
	for i := len(s.Entries) - 1; i >= 0; i-- {
		entry := s.Entries[i]
		if end != "+" && compareIDs(entry.ID, end) > 0 {
			continue
		}
		if start != "-" && compareIDs(entry.ID, start) < 0 {
			break
		}
		res = append(res, entry)
	}
	return res, nil
}

// XRead reads entries ensuring ID > lastID.
// streams: map[key]lastID
// count: max entries per stream (optional, simplified to 0=all)
// block: 0 (non-blocking only for this iteration)
func (d *DistributedMap) XRead(streams map[string]string, count int) (map[string][]StreamEntry, error) {
	result := make(map[string][]StreamEntry)

	for key, lastID := range streams {
		s, err := d.getStream(key)
		if err != nil {
			return nil, err
		}
		if s == nil {
			continue
		}

		s.mu.RLock()
		entries := make([]StreamEntry, 0)
		for _, entry := range s.Entries {
			if compareIDs(entry.ID, lastID) > 0 {
				entries = append(entries, entry)
				if count > 0 && len(entries) >= count {
					break
				}
			}
		}
		s.mu.RUnlock()

		if len(entries) > 0 {
			result[key] = entries
		}
	}

	return result, nil
}
