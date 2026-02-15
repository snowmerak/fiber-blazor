package ledis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type StreamEntry struct {
	ID     string
	Fields []string // Key, Value, Key, Value...
}

type Stream struct {
	Entries []StreamEntry
	lastID  string
}

func newStream() *Stream {
	return &Stream{
		Entries: make([]StreamEntry, 0),
		lastID:  "0-0",
	}
}

// Helper to get stream item if exists
func (d *DistributedMap) getStreamItem(key string) (*Item, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return nil, nil // Not found
	}

	item := val.(*Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		shard.Delete(key)
		d.NotifyObservers(key)
		// item.reset()
		// itemPool.Put(item)
		return nil, nil
	}

	if item.Type != TypeStream {
		return nil, ErrWrongType
	}
	return item, nil
}

// Helper to get or create a stream item
func (d *DistributedMap) getOrCreateStreamItem(key string) (*Item, error) {
	shard := d.getShard(key)
	val, loaded := shard.Load(key)

	if loaded {
		item := val.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			shard.Delete(key)
			d.NotifyObservers(key)
			// item.reset()
			// itemPool.Put(item)
			loaded = false
		} else {
			if item.Type != TypeStream {
				return nil, ErrWrongType
			}
			return item, nil
		}
	}

	// Create new
	newItem := itemPool.Get().(*Item)
	newItem.reset()
	newItem.Type = TypeStream
	newItem.Stream = newStream()
	newItem.ExpiresAt = 0

	actual, loaded := shard.LoadOrStore(key, newItem)
	if loaded {
		newItem.reset()
		itemPool.Put(newItem)

		item := actual.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			return d.getOrCreateStreamItem(key)
		}
		if item.Type != TypeStream {
			return nil, ErrWrongType
		}
		return item, nil
	}

	d.NotifyObservers(key)
	return newItem, nil
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
			ts = lastTs
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
// maxLen: 0 for no limit, >0 for exact limit.
func (d *DistributedMap) XAdd(key string, id string, maxLen int64, fields ...string) (string, error) {
	if len(fields)%2 != 0 {
		return "", errors.New("wrong number of arguments for XADD")
	}

	item, err := d.getOrCreateStreamItem(key)
	if err != nil {
		return "", err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	s := item.Stream
	if s == nil {
		s = newStream()
		item.Stream = s
	}

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

	// Trim if needed
	if maxLen > 0 && int64(len(s.Entries)) > maxLen {
		// Remove from head
		start := int64(len(s.Entries)) - maxLen
		if start > 0 {
			s.Entries = s.Entries[start:]
		}
	}

	return newID, nil
}

// XTrim trims the stream to maxLen.
// Returns the number of entries deleted.
func (d *DistributedMap) XTrim(key string, maxLen int64) (int64, error) {
	if maxLen < 0 {
		return 0, errors.New("maxLen must be >= 0")
	}

	item, err := d.getStreamItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	s := item.Stream
	if s == nil {
		return 0, nil
	}

	currentLen := int64(len(s.Entries))
	if currentLen <= maxLen {
		return 0, nil
	}

	removeCount := currentLen - maxLen
	s.Entries = s.Entries[removeCount:]

	return removeCount, nil
}

// XLen returns the number of entries in the stream.
func (d *DistributedMap) XLen(key string) (int64, error) {
	item, err := d.getStreamItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	s := item.Stream
	if s == nil {
		return 0, nil
	}

	return int64(len(s.Entries)), nil
}

// XRange returns entries within a range [start, end].
func (d *DistributedMap) XRange(key, start, end string) ([]StreamEntry, error) {
	item, err := d.getStreamItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []StreamEntry{}, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	s := item.Stream
	if s == nil {
		return []StreamEntry{}, nil
	}

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
	item, err := d.getStreamItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []StreamEntry{}, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	s := item.Stream
	if s == nil {
		return []StreamEntry{}, nil
	}

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
		item, err := d.getStreamItem(key)
		if err != nil {
			return nil, err
		}
		if item == nil {
			continue
		}

		item.Mu.RLock()
		s := item.Stream
		if s == nil {
			item.Mu.RUnlock()
			continue
		}

		entries := make([]StreamEntry, 0)
		for _, entry := range s.Entries {
			if compareIDs(entry.ID, lastID) > 0 {
				entries = append(entries, entry)
				if count > 0 && len(entries) >= count {
					break
				}
			}
		}
		item.Mu.RUnlock()

		if len(entries) > 0 {
			result[key] = entries
		}
	}

	return result, nil
}
