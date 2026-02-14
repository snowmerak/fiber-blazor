package ledis

import (
	"errors"
	"strconv"
	"time"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrNotString  = errors.New("value is not a string")
)

// GetSet sets the given key to value and returns the old value.
func (d *DistributedMap) GetSet(key string, value interface{}) (interface{}, bool) {
	shard := d.getShard(key)

	val, loaded := shard.Load(key)

	shard.Store(key, Item{
		Value:     value,
		ExpiresAt: 0, // GETSET usually clears TTL or persists? Redis persists. Let's assume persist for now or modify `Set` to keep it.
		// Actually Redis spec says: "GETSET sets ... and returns the old value." "The TTL is discarded".
		// So ExpiresAt: 0 is correct for "persist" / no expiry.
	})

	if !loaded {
		return nil, false
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		return nil, false
	}

	return item.Value, true
}

// MSet sets multiple keys to multiple values.
func (d *DistributedMap) MSet(pairs map[string]interface{}) {
	for k, v := range pairs {
		d.Set(k, v, 0)
	}
}

// MGet returns the values of all specified keys.
// For every key that does not hold a string value or does not exist, the special value nil is returned.
func (d *DistributedMap) MGet(keys ...string) []interface{} {
	values := make([]interface{}, len(keys))
	for i, key := range keys {
		val, ok := d.Get(key)
		if ok {
			values[i] = val
		} else {
			values[i] = nil
		}
	}
	return values
}

// Incr increments the number stored at key by one.
func (d *DistributedMap) Incr(key string) (int64, error) {
	return d.IncrBy(key, 1)
}

// Decr decrements the number stored at key by one.
func (d *DistributedMap) Decr(key string) (int64, error) {
	return d.IncrBy(key, -1)
}

// IncrBy increments the number stored at key by increment.
func (d *DistributedMap) IncrBy(key string, amount int64) (int64, error) {
	shard := d.getShard(key)
	for {
		rawVal, loaded := shard.Load(key)

		var current int64
		var expiresAt int64

		if !loaded {
			current = 0
			expiresAt = 0
		} else {
			item := rawVal.(Item)
			if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
				// Expired, treat as new
				current = 0
				expiresAt = 0
			} else {
				var err error
				current, err = toInt64(item.Value)
				if err != nil {
					return 0, err
				}
				expiresAt = item.ExpiresAt
			}
		}

		newValue := current + amount
		newItem := Item{Value: newValue, ExpiresAt: expiresAt}

		if loaded {
			if shard.CompareAndSwap(key, rawVal, newItem) {
				return newValue, nil
			}
		} else {
			_, loadedAgain := shard.LoadOrStore(key, newItem)
			if !loadedAgain {
				return newValue, nil
			}
		}
	}
}

// DecrBy decrements the number stored at key by decrement.
func (d *DistributedMap) DecrBy(key string, amount int64) (int64, error) {
	return d.IncrBy(key, -amount)
}

// Append appends the value at the end of the string.
func (d *DistributedMap) Append(key string, value string) (int, error) {
	for {
		shard := d.getShard(key)
		rawVal, rawOk := shard.Load(key)

		var current string
		var expiresAt int64

		if rawOk {
			item := rawVal.(Item)
			if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
				// Expired
				current = ""
				expiresAt = 0
			} else {
				strVal, ok := item.Value.(string)
				if !ok {
					return 0, ErrNotString
				}
				current = strVal
				expiresAt = item.ExpiresAt
			}
		} else {
			current = ""
			expiresAt = 0
		}

		newValue := current + value
		newItem := Item{Value: newValue, ExpiresAt: expiresAt}

		if rawOk {
			if shard.CompareAndSwap(key, rawVal, newItem) {
				return len(newValue), nil
			}
		} else {
			actual, loaded := shard.LoadOrStore(key, newItem)
			if !loaded {
				return len(newValue), nil
			}
			// If loaded, retry
			_ = actual
		}
	}
}

// StrLen returns the length of the string value stored at key.
func (d *DistributedMap) StrLen(key string) (int, error) {
	val, ok := d.Get(key)
	if !ok {
		return 0, nil
	}

	strVal, ok := val.(string)
	if !ok {
		return 0, ErrNotString
	}

	return len(strVal), nil
}

// Helper to convert interface{} to int64
func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		// Potential overflow if uint64 is too large for int64, but Redis strings are signed 64 bit usually.
		return int64(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		return i, nil
	default:
		return 0, ErrNotInteger
	}
}
