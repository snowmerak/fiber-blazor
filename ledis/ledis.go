package ledis

import (
	"hash/maphash"
	"math/bits"
	"sync"
	"time"
)

type Item struct {
	Value     interface{}
	ExpiresAt int64
}

type DistributedMap struct {
	shards []*sync.Map
	mask   uint64
	seed   maphash.Seed
}

func New(size int) *DistributedMap {
	if size <= 0 {
		size = 1024
	}
	// adjust size to be a power of 2
	size = 1 << bits.Len(uint(size-1))

	shards := make([]*sync.Map, size)
	for i := 0; i < size; i++ {
		shards[i] = &sync.Map{}
	}

	return &DistributedMap{
		shards: shards,
		mask:   uint64(size - 1),
		seed:   maphash.MakeSeed(),
	}
}

func (d *DistributedMap) hash(key string) uint64 {
	var h maphash.Hash
	h.SetSeed(d.seed)
	h.WriteString(key)
	return h.Sum64()
}

func (d *DistributedMap) getShard(key string) *sync.Map {
	idx := d.hash(key) & d.mask
	return d.shards[idx]
}

func (d *DistributedMap) Set(key string, value interface{}, duration time.Duration) {
	shard := d.getShard(key)
	expiresAt := int64(0)
	if duration > 0 {
		expiresAt = time.Now().Add(duration).UnixNano()
	}
	shard.Store(key, Item{
		Value:     value,
		ExpiresAt: expiresAt,
	})
}

func (d *DistributedMap) Get(key string) (interface{}, bool) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return nil, false
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		shard.Delete(key)
		return nil, false
	}

	return item.Value, true
}

func (d *DistributedMap) Del(key string) {
	shard := d.getShard(key)
	shard.Delete(key)
}

func (d *DistributedMap) Exists(key string) bool {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return false
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		shard.Delete(key)
		return false
	}

	return true
}

func (d *DistributedMap) TTL(key string) time.Duration {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return -2 // Key does not exist
	}

	item := val.(Item)
	if item.ExpiresAt == 0 {
		return -1 // No expiration
	}

	ttl := time.Duration(item.ExpiresAt - time.Now().UnixNano())
	if ttl < 0 {
		shard.Delete(key)
		return -2 // Key expired (and thus does not exist)
	}

	return ttl
}
