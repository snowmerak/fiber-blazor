package ledis

import (
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// Helper to get bitmap item if exists
func (d *DistributedMap) getBitmapItem(key string) (*Item, error) {
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

	if item.Type != TypeBitmap {
		return nil, ErrWrongType
	}
	return item, nil
}

// Helper to get or create a bitmap item
func (d *DistributedMap) getOrCreateBitmapItem(key string) (*Item, error) {
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
			if item.Type != TypeBitmap {
				return nil, ErrWrongType
			}
			return item, nil
		}
	}

	// Create new
	newItem := itemPool.Get().(*Item)
	newItem.reset()
	newItem.Type = TypeBitmap
	newItem.Bitmap = roaring64.New()
	newItem.ExpiresAt = 0

	actual, loaded := shard.LoadOrStore(key, newItem)
	if loaded {
		newItem.reset()
		itemPool.Put(newItem)

		item := actual.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			return d.getOrCreateBitmapItem(key)
		}
		if item.Type != TypeBitmap {
			return nil, ErrWrongType
		}
		return item, nil
	}

	d.NotifyObservers(key)
	return newItem, nil
}

// SetBit sets or clears the bit at offset in the string value stored at key.
func (d *DistributedMap) SetBit(key string, offset uint64, value int) (int, error) {
	item, err := d.getOrCreateBitmapItem(key)
	if err != nil {
		return 0, err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	if item.Bitmap == nil {
		item.Bitmap = roaring64.New()
	}

	original := 0
	if item.Bitmap.Contains(offset) {
		original = 1
	}

	if value == 1 {
		item.Bitmap.Add(offset)
	} else {
		item.Bitmap.Remove(offset)
	}

	return original, nil
}

// GetBit returns the bit value at offset in the string value stored at key.
func (d *DistributedMap) GetBit(key string, offset uint64) (int, error) {
	item, err := d.getBitmapItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	if item.Bitmap != nil && item.Bitmap.Contains(offset) {
		return 1, nil
	}
	return 0, nil
}

// BitCount performs a population count (popcount) on the bitmap.
func (d *DistributedMap) BitCount(key string) (uint64, error) {
	item, err := d.getBitmapItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	if item.Bitmap == nil {
		return 0, nil
	}

	return item.Bitmap.GetCardinality(), nil
}

// BitOp performs a bitwise operation between multiple keys (destKey = op key1 key2 ...)
// Operations: AND, OR, XOR, NOT (NOT only takes 1 key)
func (d *DistributedMap) BitOp(op string, destKey string, keys ...string) (int64, error) {
	if op == "NOT" {
		if len(keys) != 1 {
			return 0, nil // NOT requires exactly 1 key
		}

		srcItem, err := d.getBitmapItem(keys[0])
		if err != nil {
			return 0, err
		}

		dest := roaring64.New()
		if srcItem != nil {
			srcItem.Mu.RLock()
			if srcItem.Bitmap != nil {
				// Clone first
				dest = srcItem.Bitmap.Clone()
				// Find max
				if !dest.IsEmpty() {
					max := dest.Maximum()
					dest.Flip(0, max+1) // Flip 0..Max
				}
			}
			srcItem.Mu.RUnlock()
		}

		d.Del(destKey)

		// Save
		newItem := itemPool.Get().(*Item)
		newItem.reset()
		newItem.Type = TypeBitmap
		newItem.Bitmap = dest
		newItem.ExpiresAt = 0

		shard := d.getShard(destKey)
		shard.Store(destKey, newItem)
		d.NotifyObservers(destKey)

		return int64(dest.GetCardinality()), nil
	}

	// For AND, OR, XOR
	res := roaring64.New()

	first := true

	for _, k := range keys {
		item, err := d.getBitmapItem(k)
		if err != nil {
			return 0, err
		}

		if item == nil {
			if op == "AND" {
				res.Clear()
				break
			}
			continue
		}

		item.Mu.RLock()
		if item.Bitmap == nil {
			item.Mu.RUnlock()
			if op == "AND" {
				res.Clear()
				break
			}
			continue
		}

		if first {
			res = item.Bitmap.Clone()
			first = false
		} else {
			switch op {
			case "AND":
				res.And(item.Bitmap)
			case "OR":
				res.Or(item.Bitmap)
			case "XOR":
				res.Xor(item.Bitmap)
			}
		}
		item.Mu.RUnlock()
	}

	if op == "AND" && first {
		// Means all keys were missing. AND result is empty.
		res.Clear()
	}

	d.Del(destKey)
	if res.GetCardinality() > 0 {
		newItem := itemPool.Get().(*Item)
		newItem.reset()
		newItem.Type = TypeBitmap
		newItem.Bitmap = res
		newItem.ExpiresAt = 0

		shard := d.getShard(destKey)
		shard.Store(destKey, newItem)
		d.NotifyObservers(destKey)

		return int64(res.GetCardinality()), nil
	}
	return 0, nil
}
