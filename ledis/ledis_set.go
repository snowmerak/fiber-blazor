package ledis

import (
	"fmt"
	"math/rand"
	"time"
)

// Helper to get set item if exists
func (d *DistributedMap) getSetItem(key string) (*Item, error) {
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

	if item.Type != TypeSet {
		return nil, ErrWrongType
	}
	return item, nil
}

// Helper to get or create a set item
func (d *DistributedMap) getOrCreateSetItem(key string) (*Item, error) {
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
			if item.Type != TypeSet {
				return nil, ErrWrongType
			}
			return item, nil
		}
	}

	// Create new
	newItem := itemPool.Get().(*Item)
	newItem.reset()
	newItem.Type = TypeSet
	newItem.Set = make(map[string]struct{})
	newItem.ExpiresAt = 0

	actual, loaded := shard.LoadOrStore(key, newItem)
	if loaded {
		newItem.reset()
		itemPool.Put(newItem)

		item := actual.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			return d.getOrCreateSetItem(key)
		}
		if item.Type != TypeSet {
			return nil, ErrWrongType
		}
		return item, nil
	}

	d.NotifyObservers(key)
	return newItem, nil
}

// SAdd adds the specified members to the set stored at key.
func (d *DistributedMap) SAdd(key string, members ...any) (int, error) {
	item, err := d.getOrCreateSetItem(key)
	if err != nil {
		return 0, err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	if item.Set == nil {
		item.Set = make(map[string]struct{})
	}

	added := 0
	for _, m := range members {
		strVal := ""
		switch v := m.(type) {
		case string:
			strVal = v
		default:
			strVal = fmt.Sprintf("%v", v)
		}

		if _, exists := item.Set[strVal]; !exists {
			item.Set[strVal] = struct{}{}
			added++
		}
	}
	return added, nil
}

// SRem removes the specified members from the set stored at key.
func (d *DistributedMap) SRem(key string, members ...any) (int, error) {
	item, err := d.getSetItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.Lock()

	removed := 0
	for _, m := range members {
		strVal := ""
		switch v := m.(type) {
		case string:
			strVal = v
		default:
			strVal = fmt.Sprintf("%v", v)
		}

		if _, exists := item.Set[strVal]; exists {
			delete(item.Set, strVal)
			removed++
		}
	}

	isEmpty := len(item.Set) == 0
	item.Mu.Unlock()

	if isEmpty {
		d.Del(key)
	}

	return removed, nil
}

// SIsMember returns if member is a member of the set stored at key.
func (d *DistributedMap) SIsMember(key string, member any) (bool, error) {
	strVal := ""
	switch v := member.(type) {
	case string:
		strVal = v
	default:
		strVal = fmt.Sprintf("%v", v)
	}

	item, err := d.getSetItem(key)
	if err != nil {
		return false, err
	}
	if item == nil {
		return false, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	_, exists := item.Set[strVal]
	return exists, nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (d *DistributedMap) SCard(key string) (int, error) {
	item, err := d.getSetItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	return len(item.Set), nil
}

// SMembers returns all the members of the set value stored at key.
func (d *DistributedMap) SMembers(key string) ([]any, error) {
	item, err := d.getSetItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []any{}, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	members := make([]any, 0, len(item.Set))
	for m := range item.Set {
		members = append(members, m)
	}
	return members, nil
}

// SMove moves member from the set at source to the set at destination.
func (d *DistributedMap) SMove(source, destination string, member any) (bool, error) {
	exists, err := d.SIsMember(source, member)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}

	n, err := d.SRem(source, member)
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil // Lost race?
	}

	_, err = d.SAdd(destination, member)
	if err != nil {
		d.SAdd(source, member)
		return false, err
	}

	return true, nil
}

// SPop removes and returns a random member from the set value stored at key.
func (d *DistributedMap) SPop(key string) (any, error) {
	item, err := d.getSetItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	item.Mu.Lock()

	var member string
	for m := range item.Set {
		member = m
		break // Pick first one (random-ish)
	}

	delete(item.Set, member)

	isEmpty := len(item.Set) == 0
	item.Mu.Unlock()

	if isEmpty {
		d.Del(key)
	}

	return member, nil
}

// SRandMember returns a random member from the set value stored at key.
func (d *DistributedMap) SRandMember(key string, count int) ([]any, error) {
	item, err := d.getSetItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	if len(item.Set) == 0 {
		return nil, nil
	}

	if count == 0 {
		return []any{}, nil
	}

	result := make([]any, 0)

	if count > 0 {
		if count >= len(item.Set) {
			for m := range item.Set {
				result = append(result, m)
			}
			return result, nil
		}

		c := 0
		for m := range item.Set {
			result = append(result, m)
			c++
			if c >= count {
				break
			}
		}
	} else {
		count = -count
		keys := make([]string, 0, len(item.Set))
		for m := range item.Set {
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
func (d *DistributedMap) SDiff(keys ...string) ([]any, error) {
	if len(keys) == 0 {
		return []any{}, nil
	}

	// Fetch all sets
	sets := make([]*Item, len(keys))
	for i, k := range keys {
		s, err := d.getSetItem(k)
		if err != nil {
			return nil, err
		}
		sets[i] = s
	}

	if sets[0] == nil {
		return []any{}, nil
	}

	base := make(map[string]struct{})
	sets[0].Mu.RLock()
	for m := range sets[0].Set {
		base[m] = struct{}{}
	}
	sets[0].Mu.RUnlock()

	for i := 1; i < len(sets); i++ {
		s := sets[i]
		if s == nil {
			continue
		}
		s.Mu.RLock()
		for m := range s.Set {
			delete(base, m)
		}
		s.Mu.RUnlock()
	}

	result := make([]any, 0, len(base))
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

func (d *DistributedMap) SInter(keys ...string) ([]any, error) {
	if len(keys) == 0 {
		return []any{}, nil
	}

	// Fetch all
	sets := make([]*Item, len(keys))
	for i, k := range keys {
		s, err := d.getSetItem(k)
		if err != nil {
			return nil, err
		}
		sets[i] = s
		if s == nil {
			return []any{}, nil
		}
	}

	base := make(map[string]struct{})
	sets[0].Mu.RLock()
	for m := range sets[0].Set {
		base[m] = struct{}{}
	}
	sets[0].Mu.RUnlock()

	for i := 1; i < len(sets); i++ {
		s := sets[i]
		nextBase := make(map[string]struct{})
		s.Mu.RLock()
		for m := range base {
			if _, exists := s.Set[m]; exists {
				nextBase[m] = struct{}{}
			}
		}
		s.Mu.RUnlock()
		base = nextBase
		if len(base) == 0 {
			break
		}
	}

	result := make([]any, 0, len(base))
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

func (d *DistributedMap) SUnion(keys ...string) ([]any, error) {
	if len(keys) == 0 {
		return []any{}, nil
	}

	base := make(map[string]struct{})

	for _, k := range keys {
		s, err := d.getSetItem(k)
		if err != nil {
			return nil, err
		}
		if s == nil {
			continue
		}
		s.Mu.RLock()
		for m := range s.Set {
			base[m] = struct{}{}
		}
		s.Mu.RUnlock()
	}

	result := make([]any, 0, len(base))
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
