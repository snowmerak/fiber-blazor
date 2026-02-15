package ledis

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrNotString  = errors.New("value is not a string")
)

// GetSet sets the given key to value and returns the old value.
func (d *DistributedMap) GetSet(key string, value any) (any, bool) {
	// Value must be string for now, or we convert
	strVal := ""
	switch v := value.(type) {
	case string:
		strVal = v
	default:
		strVal = fmt.Sprintf("%v", v)
	}

	shard := d.getShard(key)

	newItem := itemPool.Get().(*Item)
	newItem.reset()
	newItem.Type = TypeString
	newItem.Str = strVal
	newItem.ExpiresAt = 0

	// Swap
	// Use LoadOrStore? No, we want to replace always.
	// Map.Swap is available in go 1.20+? Or standard sync.Map has Swap?
	// sync.Map has Swap since Go 1.20. User environment?
	// If not, we use Store, but we need old value to return AND to free.
	// If we use Store, we can't atomically get Ref to old to free it safely if concurrents are accessing?
	// Actually, Store returns nothing.
	// But `Load` then `Store` is not atomic.
	// Wait, `Swap` IS atomic and returns previous value.
	// Let's assume Go 1.20+ (safe assumption for modern dev).
	// If compilation fails, I'll fix.

	previous, loaded := shard.Swap(key, newItem)

	if loaded {
		prevItem := previous.(*Item)
		// We return the value... but we also need to free the Item struct?
		// We can't free it if we return a reference to its field (like &prevItem.Str).
		// But we return `interface{}` which is `prevItem.Str` (copy of string header, and string data is on heap).
		// So it is safe to return `prevItem.Str` and then `Put(prevItem)`.
		// BE CAREFUL: string data is immutable.
		// What if return type was map/slice? Copying map/slice header is fine, but backing array is shared.
		// If we recycle `prevItem`, we reset it. `prevItem.List = nil`.
		// If the user of `GetSet` (the caller) holds the slice `prevItem.List`, and we `reset` -> `List=nil`,
		// that's fine, the caller has their own slice header pointing to backing array.
		// `reset` only clears the struct fields.
		// So it is SAFE.

		val := prevItem.Str
		if prevItem.Type != TypeString {
			// Handle mismatch? Redis returns error or converts?
			// GETSET usually for strings.
			// Just return whatever it was?
			// Verify if it was string?
			// If not string, maybe return nil/error?
			// For now return val if type match, else we might have issue.
			// But we are overwriting anyway.
			// We'll return based on type.
		}

		// If expired?
		if prevItem.ExpiresAt > 0 && prevItem.ExpiresAt < time.Now().UnixNano() {
			val = "" // Expired
			loaded = false
		} else {
			// Extract value
			switch prevItem.Type {
			case TypeString:
				val = prevItem.Str
			default:
				// Return string repr?
				val = "" // or handle conversion
			}
		}

		// prevItem.reset()
		// itemPool.Put(prevItem)
		return val, loaded
	}

	d.NotifyObservers(key)
	return nil, false
}

// MSet sets multiple keys to multiple values.
func (d *DistributedMap) MSet(pairs map[string]any) {
	for k, v := range pairs {
		d.Set(k, v, 0)
	}
}

// MGet returns the values of all specified keys.
func (d *DistributedMap) MGet(keys ...string) []any {
	values := make([]any, len(keys))
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
