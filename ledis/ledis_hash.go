package ledis

import (
	"errors"
	"sync"
	"time"
)

type Hash struct {
	mu   sync.RWMutex
	Data map[string]interface{}
}

func NewHash() *Hash {
	return &Hash{
		Data: make(map[string]interface{}),
	}
}

// Helper to get or create a hash
func (d *DistributedMap) getOrCreateHash(key string) (*Hash, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		h := NewHash()
		val, loaded := shard.LoadOrStore(key, Item{Value: h, ExpiresAt: 0})
		if loaded {
			item := val.(Item)
			if hVal, ok := item.Value.(*Hash); ok {
				return hVal, nil
			}
			return nil, ErrWrongType
		}
		return h, nil
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		h := NewHash()
		shard.Store(key, Item{Value: h, ExpiresAt: 0})
		return h, nil
	}

	h, ok := item.Value.(*Hash)
	if !ok {
		return nil, ErrWrongType
	}
	return h, nil
}

// Helper to get hash if exists
func (d *DistributedMap) getHash(key string) (*Hash, error) {
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

	h, ok := item.Value.(*Hash)
	if !ok {
		return nil, ErrWrongType
	}
	return h, nil
}

// HSet sets field in the hash stored at key to value.
func (d *DistributedMap) HSet(key string, field string, value interface{}) (int, error) {
	h, err := d.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	_, exists := h.Data[field]
	h.Data[field] = value

	if exists {
		return 0, nil
	}
	return 1, nil
}

// HGet returns the value associated with field in the hash stored at key.
func (d *DistributedMap) HGet(key string, field string) (interface{}, error) {
	h, err := d.getHash(key)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.Data[field], nil
}

// HDel removes the specified fields from the hash stored at key.
func (d *DistributedMap) HDel(key string, fields ...string) (int, error) {
	h, err := d.getHash(key)
	if err != nil {
		return 0, err
	}
	if h == nil {
		return 0, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	count := 0
	for _, f := range fields {
		if _, ok := h.Data[f]; ok {
			delete(h.Data, f)
			count++
		}
	}

	if len(h.Data) == 0 {
		d.Del(key)
	}

	return count, nil
}

// HExists returns if field is an existing field in the hash.
func (d *DistributedMap) HExists(key string, field string) (bool, error) {
	h, err := d.getHash(key)
	if err != nil {
		return false, err
	}
	if h == nil {
		return false, nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.Data[field]
	return ok, nil
}

// HLen returns the number of fields contained in the hash.
func (d *DistributedMap) HLen(key string) (int, error) {
	h, err := d.getHash(key)
	if err != nil {
		return 0, err
	}
	if h == nil {
		return 0, nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.Data), nil
}

// HMSet sets the specified fields to their respective values.
func (d *DistributedMap) HMSet(key string, pairs map[string]interface{}) error {
	h, err := d.getOrCreateHash(key)
	if err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for k, v := range pairs {
		h.Data[k] = v
	}
	return nil
}

// HMGet returns the values associated with the specified fields.
func (d *DistributedMap) HMGet(key string, fields ...string) ([]interface{}, error) {
	h, err := d.getHash(key)
	if err != nil {
		return nil, err
	}

	result := make([]interface{}, len(fields))

	if h == nil {
		return result, nil // All nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	for i, f := range fields {
		result[i] = h.Data[f]
	}
	return result, nil
}

// HGetAll returns all fields and values of the hash.
func (d *DistributedMap) HGetAll(key string) (map[string]interface{}, error) {
	h, err := d.getHash(key)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return make(map[string]interface{}), nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	// Copy to match snapshot isolation semantic
	result := make(map[string]interface{}, len(h.Data))
	for k, v := range h.Data {
		result[k] = v
	}
	return result, nil
}

// HKeys returns all field names in the hash.
func (d *DistributedMap) HKeys(key string) ([]string, error) {
	h, err := d.getHash(key)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return []string{}, nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	keys := make([]string, 0, len(h.Data))
	for k := range h.Data {
		keys = append(keys, k)
	}
	return keys, nil
}

// HVals returns all values in the hash.
func (d *DistributedMap) HVals(key string) ([]interface{}, error) {
	h, err := d.getHash(key)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return []interface{}{}, nil
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	vals := make([]interface{}, 0, len(h.Data))
	for _, v := range h.Data {
		vals = append(vals, v)
	}
	return vals, nil
}

// HIncrBy increments the integer value of a hash field by the given number.
func (d *DistributedMap) HIncrBy(key string, field string, amount int64) (int64, error) {
	h, err := d.getOrCreateHash(key)
	if err != nil {
		return 0, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	val, ok := h.Data[field]
	var current int64
	if !ok {
		current = 0
	} else {
		// reuse toInt64 from ledis_string.go?
		// it is in the same package `ledis`.
		current, err = toInt64(val)
		if err != nil {
			return 0, err
		}
	}

	newValue := current + amount
	h.Data[field] = newValue
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
