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
// GetSet sets the given key to value and returns the old value.
func (d *DistributedMap) GetSet(key string, value string) (string, bool, error) {
	shard := d.getShard(key)

	newItem := itemPool.Get().(*Item)
	newItem.reset()
	newItem.Type = TypeString
	newItem.Str = value
	newItem.ExpiresAt = 0

	previous, loaded := shard.Swap(key, newItem)

	if loaded {
		prevItem := previous.(*Item)
		val := ""
		if prevItem.Type == TypeString {
			val = prevItem.Str
		}

		// check expiry? If expired, we treat as not loaded/new?
		// But Swap already happened. We just return empty string and false?
		// Or false means "no previous value".
		// Redis GETSET returns nil if key didn't exist.
		// If key existed but expired, we should return nil (false).
		if prevItem.ExpiresAt > 0 && prevItem.ExpiresAt < time.Now().UnixNano() {
			// It was expired.
			loaded = false
			val = ""
		}

		// We can't easily recycle prevItem safely here if we don't know if others are using it.
		// Current design assumes Swap makes us owner?
		// Yes, if we are the only one removing it from map.
		// But readers might still hold pointer.
		// So we rely on GC or ref counting (which we don't have).
		// We'll let GC handle it.

		return val, loaded, nil
	}

	d.NotifyObservers(key)
	return "", false, nil
}

// MSet sets multiple keys to multiple values.
func (d *DistributedMap) MSet(pairs map[string]any) {
	for k, v := range pairs {
		d.Set(k, v, 0)
	}
}

// MGet returns the values of all specified keys.
// MGet returns the values of all specified keys.
// Caller MUST Lock/RLock each item.Mu before accessing fields.
func (d *DistributedMap) MGet(keys ...string) []*Item {
	values := make([]*Item, len(keys))
	for i, key := range keys {
		item, err := d.Get(key)
		if err == nil {
			values[i] = item
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
		var oldItem *Item

		if loaded {
			oldItem = rawVal.(*Item)
			if oldItem.ExpiresAt > 0 && oldItem.ExpiresAt < time.Now().UnixNano() {
				// Expired
				current = 0
				expiresAt = 0
				// Don't use oldItem for stats, treat as new.
				// But we DO NOT free oldItem here because we haven't successfully replaced it yet!
				// We only free if we CAS successfully or if we abandon.
			} else {
				if oldItem.Type != TypeString {
					return 0, ErrNotInteger // Wrong Type
				}
				// Parse
				i, err := strconv.ParseInt(oldItem.Str, 10, 64)
				if err != nil {
					return 0, ErrNotInteger
				}
				current = i
				expiresAt = oldItem.ExpiresAt
			}
		}

		newValue := current + amount
		newStr := strconv.FormatInt(newValue, 10)

		newItem := itemPool.Get().(*Item)
		newItem.reset()
		newItem.Type = TypeString
		newItem.Str = newStr
		newItem.ExpiresAt = expiresAt

		if loaded {
			if shard.CompareAndSwap(key, rawVal, newItem) {
				// Success! Now we own rawVal/oldItem.
				// But we DO NOT recycle it because others might have pointers to it.
				d.NotifyObservers(key)
				return newValue, nil
			}
			// CAS failed.
			// Free newItem and modify loop to retry
			newItem.reset()
			itemPool.Put(newItem)
			// Loop continues
		} else {
			actual, loadedAgain := shard.LoadOrStore(key, newItem)
			if !loadedAgain {
				// Success (Store happened)
				d.NotifyObservers(key)
				return newValue, nil
			}
			// Store failed (someone else stored).
			// Free newItem and retry
			newItem.reset()
			itemPool.Put(newItem)
			_ = actual
			// Loop continues
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
		var oldItem *Item

		if rawOk {
			oldItem = rawVal.(*Item)
			if oldItem.ExpiresAt > 0 && oldItem.ExpiresAt < time.Now().UnixNano() {
				// Expired
				current = ""
				expiresAt = 0
			} else {
				if oldItem.Type != TypeString {
					return 0, ErrNotString
				}
				current = oldItem.Str
				expiresAt = oldItem.ExpiresAt
			}
		} else {
			current = ""
			expiresAt = 0
		}

		newValue := current + value
		newItem := itemPool.Get().(*Item)
		newItem.reset()
		newItem.Type = TypeString
		newItem.Str = newValue
		newItem.ExpiresAt = expiresAt

		if rawOk {
			if shard.CompareAndSwap(key, rawVal, newItem) {
				d.NotifyObservers(key)
				return len(newValue), nil
			}
			newItem.reset()
			itemPool.Put(newItem)
		} else {
			actual, loaded := shard.LoadOrStore(key, newItem)
			if !loaded {
				d.NotifyObservers(key)
				return len(newValue), nil
			}
			newItem.reset()
			itemPool.Put(newItem)
			_ = actual
		}
	}
}

// StrLen returns the length of the string value stored at key.
func (d *DistributedMap) StrLen(key string) (int, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return 0, nil
	}

	item := val.(*Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		shard.Delete(key)
		d.NotifyObservers(key)
		item.reset()
		itemPool.Put(item)
		return 0, nil
	}

	if item.Type != TypeString {
		return 0, ErrNotString
	}

	return len(item.Str), nil
}

// Helper to convert interface{} to int64 (Removed, not needed with strict typing)
