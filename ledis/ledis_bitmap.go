package ledis

import (
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type Bitmap struct {
	mu   sync.RWMutex
	Data *roaring64.Bitmap
}

func NewBitmap() *Bitmap {
	return &Bitmap{
		Data: roaring64.New(),
	}
}

// Helper to get or create Bitmap
func (d *DistributedMap) getOrCreateBitmap(key string) (*Bitmap, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		b := NewBitmap()
		val, loaded := shard.LoadOrStore(key, Item{Value: b, ExpiresAt: 0})
		if loaded {
			item := val.(Item)
			if bVal, ok := item.Value.(*Bitmap); ok {
				return bVal, nil
			}
			return nil, ErrWrongType
		}
		return b, nil
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		b := NewBitmap()
		shard.Store(key, Item{Value: b, ExpiresAt: 0})
		return b, nil
	}

	b, ok := item.Value.(*Bitmap)
	if !ok {
		return nil, ErrWrongType
	}
	return b, nil
}

// Helper to get Bitmap if exists
func (d *DistributedMap) getBitmap(key string) (*Bitmap, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return nil, nil // Not found
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		d.Del(key)
		return nil, nil
	}

	b, ok := item.Value.(*Bitmap)
	if !ok {
		return nil, ErrWrongType
	}
	return b, nil
}

// SetBit sets or clears the bit at offset in the string value stored at key.
func (d *DistributedMap) SetBit(key string, offset uint64, value int) (int, error) {
	b, err := d.getOrCreateBitmap(key)
	if err != nil {
		return 0, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	original := 0
	if b.Data.Contains(offset) {
		original = 1
	}

	if value == 1 {
		b.Data.Add(offset)
	} else {
		b.Data.Remove(offset)
	}

	return original, nil
}

// GetBit returns the bit value at offset in the string value stored at key.
func (d *DistributedMap) GetBit(key string, offset uint64) (int, error) {
	b, err := d.getBitmap(key)
	if err != nil {
		return 0, err
	}
	if b == nil {
		return 0, nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.Data.Contains(offset) {
		return 1, nil
	}
	return 0, nil
}

// BitCount performs a population count (popcount) on the bitmap.
func (d *DistributedMap) BitCount(key string) (uint64, error) {
	b, err := d.getBitmap(key)
	if err != nil {
		return 0, err
	}
	if b == nil {
		return 0, nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.Data.GetCardinality(), nil
}

// BitOp performs a bitwise operation between multiple keys (destKey = op key1 key2 ...)
// Operations: AND, OR, XOR, NOT (NOT only takes 1 key)
func (d *DistributedMap) BitOp(op string, destKey string, keys ...string) (int64, error) {
	if op == "NOT" {
		if len(keys) != 1 {
			return 0, nil // NOT requires exactly 1 key
		}
		// Logic for NOT on a bitmap?
		// Roaring bitmaps are sparse. NOT on a sparse bitmap essentially makes it dense?
		// Or does it mean specific range? Redis BitOp NOT inverts bits.
		// Roaring 'Flip' operation requires a range.
		// Redis string logic: NOT inverts bytes.
		// For sparse bitmaps, "NOT" is ambiguous without a universe size.
		// If we treat it as infinite 0s, NOT makes infinite 1s.
		// However, typical use case is flip within range 0..MaxSetBit?
		// Roaring64 has func (rb *Bitmap) Flip(rangeStart, rangeEnd uint64)
		// We can support NOT by flipping from 0 to Maximum element?
		// Or just not support NOT efficiently?
		// Let's implement NOT as Flip(0, MaxKey). If empty, 0.

		src, err := d.getBitmap(keys[0])
		if err != nil {
			return 0, err
		}

		dest := roaring64.New()
		if src != nil {
			src.mu.RLock()
			// Clone first
			dest = src.Data.Clone()
			// Find max
			if !dest.IsEmpty() {
				max := dest.Maximum()
				dest.Flip(0, max+1) // Flip 0..Max
			}
			src.mu.RUnlock()
		}

		d.Del(destKey)
		// Save
		b := NewBitmap()
		b.Data = dest

		shard := d.getShard(destKey)
		shard.Store(destKey, Item{Value: b, ExpiresAt: 0})

		return int64(dest.GetCardinality()), nil
	}

	// For AND, OR, XOR
	res := roaring64.New()

	// Need to initialize 'res' correctly for AND.
	// OR/XOR start with empty is fine.
	// AND needs to start with first set? Or handle first separately.

	first := true

	for _, k := range keys {
		b, err := d.getBitmap(k)
		if err != nil {
			return 0, err
		}

		if b == nil {
			// If missing key treated as 0s.
			// AND with 0 -> 0 (res becomes empty)
			// OR with 0 -> no change
			// XOR with 0 -> no change
			if op == "AND" {
				res.Clear()
				break
			}
			continue
		}

		b.mu.RLock()
		if first {
			res = b.Data.Clone()
			first = false
		} else {
			switch op {
			case "AND":
				res.And(b.Data)
			case "OR":
				res.Or(b.Data)
			case "XOR":
				res.Xor(b.Data)
			}
		}
		b.mu.RUnlock()
	}

	if op == "AND" && first {
		// Means all keys were missing. AND result is empty.
		res.Clear()
	}

	d.Del(destKey)
	if res.GetCardinality() > 0 {
		b := NewBitmap()
		b.Data = res
		shard := d.getShard(destKey)
		shard.Store(destKey, Item{Value: b, ExpiresAt: 0})
		return int64(res.GetCardinality()), nil
	}
	return 0, nil
}
