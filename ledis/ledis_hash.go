package ledis

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Helper to get hash item if exists
func (d *DistributedMap) getHashItem(key string) (*Item, error) {
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

	if item.Type != TypeHash {
		return nil, ErrWrongType
	}
	return item, nil
}

// Helper to get or create a hash item
func (d *DistributedMap) getOrCreateHashItem(key string) (*Item, error) {
	shard := d.getShard(key)
	val, loaded := shard.Load(key)

	if loaded {
		item := val.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			// Expired, treat as new
			// We can reuse this item if we want, but "Load" returned it.
			// Simpler to delete and create new or reset it.
			// Let's reset it in place?
			// But we need to lock it to reset it safely if others are reading.
			// Or just remove and create new.
			shard.Delete(key)
			d.NotifyObservers(key)
			// item.reset()
			// itemPool.Put(item)
			loaded = false
		} else {
			if item.Type != TypeHash {
				return nil, ErrWrongType
			}
			return item, nil
		}
	}

	// Create new
	newItem := itemPool.Get().(*Item)
	newItem.reset()
	newItem.Type = TypeHash
	newItem.Hash = make(map[string]string)
	newItem.ExpiresAt = 0

	actual, loaded := shard.LoadOrStore(key, newItem)
	if loaded {
		// Race lost, use actual
		newItem.reset()
		itemPool.Put(newItem)

		item := actual.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			// Expired right after load?
			// Handle as if new?
			// Recursive retry is safest.
			return d.getOrCreateHashItem(key)
		}
		if item.Type != TypeHash {
			return nil, ErrWrongType
		}
		return item, nil
	}

	d.NotifyObservers(key)
	return newItem, nil
}

// HSet sets field in the hash stored at key to value.
func (d *DistributedMap) HSet(key string, field string, value any) (int, error) {
	// Convert value to string
	strVal := ""
	switch v := value.(type) {
	case string:
		strVal = v
	default:
		strVal = fmt.Sprintf("%v", v)
	}

	item, err := d.getOrCreateHashItem(key)
	if err != nil {
		return 0, err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	// Double check type in case of weird race/corruption (unlikely with helpers)
	if item.Hash == nil {
		item.Hash = make(map[string]string)
	}

	_, exists := item.Hash[field]
	item.Hash[field] = strVal

	if exists {
		return 0, nil
	}
	return 1, nil
}

// HGet returns the value associated with field in the hash stored at key.
func (d *DistributedMap) HGet(key string, field string) (any, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return nil, err // ErrWrongType
	}
	if item == nil {
		return nil, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	if item.Hash == nil {
		return nil, nil // Should not happen for valid TypeHash
	}

	val, ok := item.Hash[field]
	if !ok {
		return nil, nil
	}
	return val, nil
}

// HDel removes the specified fields from the hash stored at key.
func (d *DistributedMap) HDel(key string, fields ...string) (int, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.Lock()

	count := 0
	for _, f := range fields {
		if _, ok := item.Hash[f]; ok {
			delete(item.Hash, f)
			count++
		}
	}

	isEmpty := len(item.Hash) == 0
	item.Mu.Unlock()

	if isEmpty {
		d.Del(key)
	}

	return count, nil
}

// HExists returns if field is an existing field in the hash.
func (d *DistributedMap) HExists(key string, field string) (bool, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return false, err
	}
	if item == nil {
		return false, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	_, ok := item.Hash[field]
	return ok, nil
}

// HLen returns the number of fields contained in the hash.
func (d *DistributedMap) HLen(key string) (int, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	return len(item.Hash), nil
}

// HMSet sets the specified fields to their respective values.
func (d *DistributedMap) HMSet(key string, pairs map[string]any) error {
	item, err := d.getOrCreateHashItem(key)
	if err != nil {
		return err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	if item.Hash == nil {
		item.Hash = make(map[string]string)
	}

	for k, v := range pairs {
		strVal := ""
		switch val := v.(type) {
		case string:
			strVal = val
		default:
			strVal = fmt.Sprintf("%v", val)
		}
		item.Hash[k] = strVal
	}
	return nil
}

// HMGet returns the values associated with the specified fields.
func (d *DistributedMap) HMGet(key string, fields ...string) ([]any, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return nil, err
	}

	result := make([]any, len(fields))

	if item == nil {
		return result, nil // All nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	for i, f := range fields {
		if val, ok := item.Hash[f]; ok {
			result[i] = val
		} else {
			result[i] = nil
		}
	}
	return result, nil
}

// HGetAll returns all fields and values of the hash.
func (d *DistributedMap) HGetAll(key string) (map[string]any, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return make(map[string]any), nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	// Copy to match snapshot isolation semantic
	result := make(map[string]any, len(item.Hash))
	for k, v := range item.Hash {
		result[k] = v
	}
	return result, nil
}

// HKeys returns all field names in the hash.
func (d *DistributedMap) HKeys(key string) ([]string, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []string{}, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	keys := make([]string, 0, len(item.Hash))
	for k := range item.Hash {
		keys = append(keys, k)
	}
	return keys, nil
}

// HVals returns all values in the hash.
func (d *DistributedMap) HVals(key string) ([]any, error) {
	item, err := d.getHashItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []any{}, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	vals := make([]any, 0, len(item.Hash))
	for _, v := range item.Hash {
		vals = append(vals, v)
	}
	return vals, nil
}

// HIncrBy increments the integer value of a hash field by the given number.
func (d *DistributedMap) HIncrBy(key string, field string, amount int64) (int64, error) {
	item, err := d.getOrCreateHashItem(key)
	if err != nil {
		return 0, err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	if item.Hash == nil {
		item.Hash = make(map[string]string)
	}

	val, ok := item.Hash[field]
	var current int64
	if !ok {
		current = 0
	} else {
		current, err = toInt64(val) // val is string
		if err != nil {
			return 0, err
		}
	}

	newValue := current + amount
	item.Hash[field] = strconv.FormatInt(newValue, 10)
	return newValue, nil
}

// HIncrByFloat increments the float value of a hash field by the given amount.
func (d *DistributedMap) HIncrByFloat(key string, field string, amount float64) (float64, error) {
	// Not implementing float conversion helper yet, requires robust float parsing.
	// For now, let's assume if it is stored as float64, we work on it.
	// If stored as string, we parse.
	// This is standard Redis behavior.
	return 0, errors.New("not implemented yet")
}
