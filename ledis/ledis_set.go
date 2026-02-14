package ledis

import (
	"math/rand"
	"sync"
	"time"
)

type Set struct {
	mu   sync.RWMutex
	Data map[interface{}]struct{}
}

func NewSet() *Set {
	return &Set{
		Data: make(map[interface{}]struct{}),
	}
}

// Helper to get or create a set
func (d *DistributedMap) getOrCreateSet(key string) (*Set, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		s := NewSet()
		val, loaded := shard.LoadOrStore(key, Item{Value: s, ExpiresAt: 0})
		if loaded {
			item := val.(Item)
			if sVal, ok := item.Value.(*Set); ok {
				return sVal, nil
			}
			return nil, ErrWrongType
		}
		return s, nil
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		s := NewSet()
		shard.Store(key, Item{Value: s, ExpiresAt: 0})
		return s, nil
	}

	s, ok := item.Value.(*Set)
	if !ok {
		return nil, ErrWrongType
	}
	return s, nil
}

// Helper to get set if exists
func (d *DistributedMap) getSet(key string) (*Set, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return nil, nil // Not found
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		shard.Delete(key)
		return nil, nil
	}

	s, ok := item.Value.(*Set)
	if !ok {
		return nil, ErrWrongType
	}
	return s, nil
}

// SAdd adds the specified members to the set stored at key.
func (d *DistributedMap) SAdd(key string, members ...interface{}) (int, error) {
	s, err := d.getOrCreateSet(key)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	added := 0
	for _, m := range members {
		if _, exists := s.Data[m]; !exists {
			s.Data[m] = struct{}{}
			added++
		}
	}
	return added, nil
}

// SRem removes the specified members from the set stored at key.
func (d *DistributedMap) SRem(key string, members ...interface{}) (int, error) {
	s, err := d.getSet(key)
	if err != nil {
		return 0, err
	}
	if s == nil {
		return 0, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, m := range members {
		if _, exists := s.Data[m]; exists {
			delete(s.Data, m)
			removed++
		}
	}

	if len(s.Data) == 0 {
		d.Del(key)
	}

	return removed, nil
}

// SIsMember returns if member is a member of the set stored at key.
func (d *DistributedMap) SIsMember(key string, member interface{}) (bool, error) {
	s, err := d.getSet(key)
	if err != nil {
		return false, err
	}
	if s == nil {
		return false, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.Data[member]
	return exists, nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (d *DistributedMap) SCard(key string) (int, error) {
	s, err := d.getSet(key)
	if err != nil {
		return 0, err
	}
	if s == nil {
		return 0, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.Data), nil
}

// SMembers returns all the members of the set value stored at key.
func (d *DistributedMap) SMembers(key string) ([]interface{}, error) {
	s, err := d.getSet(key)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return []interface{}{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	members := make([]interface{}, 0, len(s.Data))
	for m := range s.Data {
		members = append(members, m)
	}
	return members, nil
}

// SMove moves member from the set at source to the set at destination.
func (d *DistributedMap) SMove(source, destination string, member interface{}) (bool, error) {
	// Need to lock both sets?
	// To avoid deadlocks, we should lock in consistent order or use fine-grained.
	// Simple approach: SRem from source. If successful, SAdd to destination.
	// But valid SMove requires atomic behavior?
	// Truly atomic cross-shard logic requires a global lock or careful orchestration.
	// For this simple implementation, we will do it in two steps, which is NOT atomic
	// but avoids complex locking logic.
	// If strict atomicity is required, we'd need distributed locking/transaction support.

	// Check if source has member manually to avoid side effects first?
	exists, err := d.SIsMember(source, member)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	// Remove from source
	n, err := d.SRem(source, member)
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil // Lost race?
	}

	// Add to destination
	_, err = d.SAdd(destination, member)
	if err != nil {
		// Rollback? SAdd should rarely fail if we create set.
		// If SAdd fails (WrongType), we lost the item from source!
		// We should add it back to source.
		d.SAdd(source, member)
		return false, err
	}

	return true, nil
}

// SPop removes and returns a random member from the set value stored at key.
func (d *DistributedMap) SPop(key string) (interface{}, error) {
	s, err := d.getSet(key)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}

	s.mu.Lock() // Write lock needed
	defer s.mu.Unlock()

	if len(s.Data) == 0 {
		return nil, nil
	}

	// Go map iteration is random-ish, but relying on it is valid for "random member".
	var member interface{}
	for m := range s.Data {
		member = m
		break // Pick first one
	}

	delete(s.Data, member)

	if len(s.Data) == 0 {
		d.Del(key)
	}

	return member, nil
}

// SRandMember returns a random member from the set value stored at key.
func (d *DistributedMap) SRandMember(key string, count int) ([]interface{}, error) {
	s, err := d.getSet(key)
	if err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.Data) == 0 {
		return nil, nil
	}

	// If count > 0, return count distinct elements.
	// If count < 0, return |count| elements allowing duplicates.
	// If count == 0, return empty?

	if count == 0 {
		return []interface{}{}, nil
	}

	result := make([]interface{}, 0)

	if count > 0 {
		if count >= len(s.Data) {
			// Return all
			for m := range s.Data {
				result = append(result, m)
			}
			return result, nil
		}

		// Pick count distinct
		// Basic map iteration is random enough or need proper random?
		// Map iteration order is random but not uniformly distributed cryptographic random.
		// For cached DB like strict randomness is nice but map iteration often serves as "random".
		// But if we want exactly 'count', we iterate.
		c := 0
		for m := range s.Data {
			result = append(result, m)
			c++
			if c >= count {
				break
			}
		}
	} else {
		count = -count
		// Allow duplicates.
		// Convert map keys to slice then pick random? Expensive for large sets.
		// But iterating map is linear scan.
		// If set is huge and we want 5 random items, we can't easily pick random index.
		// We have to iterate or maintain slice.
		// Trade-off: Converting to slice to pick random 5?
		// Let's do slice conversion for now.
		keys := make([]interface{}, 0, len(s.Data))
		for m := range s.Data {
			keys = append(keys, m)
		}

		for i := 0; i < count; i++ {
			idx := rand.Intn(len(keys))
			result = append(result, keys[idx])
		}
	}

	return result, nil
}

// Set Operations: SDiff, SInter, SUnion

// SDiff returns the members of the set resulting from the difference between the first set and all the successive sets.
func (d *DistributedMap) SDiff(keys ...string) ([]interface{}, error) {
	if len(keys) == 0 {
		return []interface{}{}, nil
	}

	// Fetch all sets
	sets := make([]*Set, len(keys))
	for i, k := range keys {
		s, err := d.getSet(k)
		if err != nil {
			return nil, err
		}
		sets[i] = s
	}

	if sets[0] == nil {
		return []interface{}{}, nil
	}

	// Lock all? Or Read-Lock.
	// We snapshot data.
	base := make(map[interface{}]struct{})
	sets[0].mu.RLock()
	for m := range sets[0].Data {
		base[m] = struct{}{}
	}
	sets[0].mu.RUnlock()

	for i := 1; i < len(sets); i++ {
		s := sets[i]
		if s == nil {
			continue
		}
		s.mu.RLock()
		for m := range s.Data {
			delete(base, m)
		}
		s.mu.RUnlock()
	}

	result := make([]interface{}, 0, len(base))
	for m := range base {
		result = append(result, m)
	}
	return result, nil
}

// SDiffStore is like SDiff, but instead of returning the resulting set, it is stored in destination.
func (d *DistributedMap) SDiffStore(destination string, keys ...string) (int, error) {
	diff, err := d.SDiff(keys...)
	if err != nil {
		return 0, err
	}

	d.Del(destination)
	if len(diff) > 0 {
		return d.SAdd(destination, diff...)
	}
	return 0, nil
}

func (d *DistributedMap) SInter(keys ...string) ([]interface{}, error) {
	if len(keys) == 0 {
		return []interface{}{}, nil
	}

	// Fetch all
	sets := make([]*Set, len(keys))
	for i, k := range keys {
		s, err := d.getSet(k)
		if err != nil {
			return nil, err
		}
		sets[i] = s
		if s == nil {
			// intersection with empty set is empty
			return []interface{}{}, nil
		}
	}

	// Start with first set
	// Optimization: start with smallest set?

	base := make(map[interface{}]struct{})
	sets[0].mu.RLock()
	for m := range sets[0].Data {
		base[m] = struct{}{}
	}
	sets[0].mu.RUnlock()

	for i := 1; i < len(sets); i++ {
		s := sets[i]
		nextBase := make(map[interface{}]struct{})
		s.mu.RLock()
		for m := range base {
			if _, exists := s.Data[m]; exists {
				nextBase[m] = struct{}{}
			}
		}
		s.mu.RUnlock()
		base = nextBase
		if len(base) == 0 {
			break
		}
	}

	result := make([]interface{}, 0, len(base))
	for m := range base {
		result = append(result, m)
	}
	return result, nil
}

func (d *DistributedMap) SInterStore(destination string, keys ...string) (int, error) {
	inter, err := d.SInter(keys...)
	if err != nil {
		return 0, err
	}
	d.Del(destination)
	if len(inter) > 0 {
		return d.SAdd(destination, inter...)
	}
	return 0, nil
}

func (d *DistributedMap) SUnion(keys ...string) ([]interface{}, error) {
	if len(keys) == 0 {
		return []interface{}{}, nil
	}

	base := make(map[interface{}]struct{})

	for _, k := range keys {
		s, err := d.getSet(k)
		if err != nil {
			return nil, err
		}
		if s == nil {
			continue
		}
		s.mu.RLock()
		for m := range s.Data {
			base[m] = struct{}{}
		}
		s.mu.RUnlock()
	}

	result := make([]interface{}, 0, len(base))
	for m := range base {
		result = append(result, m)
	}
	return result, nil
}

func (d *DistributedMap) SUnionStore(destination string, keys ...string) (int, error) {
	union, err := d.SUnion(keys...)
	if err != nil {
		return 0, err
	}
	d.Del(destination)
	if len(union) > 0 {
		return d.SAdd(destination, union...)
	}
	return 0, nil
}
