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
	// PubSub
	pubsub *PubSub
	// Observers for SCC
	invalidationTable map[string]map[Observer]struct{}
	clientKeys        map[Observer]map[string]struct{}
	mu                sync.RWMutex
}

type Observer interface {
	Invalidate(key string)
}

func (d *DistributedMap) RegisterObserver(o Observer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// No-op for now, or just init clientKeys entry
	if _, ok := d.clientKeys[o]; !ok {
		d.clientKeys[o] = make(map[string]struct{})
	}
}

func (d *DistributedMap) UnregisterObserver(o Observer) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. Remove from invalidationTable for all keys this client is watching
	if keys, ok := d.clientKeys[o]; ok {
		for key := range keys {
			if observers, ok := d.invalidationTable[key]; ok {
				delete(observers, o)
				if len(observers) == 0 {
					delete(d.invalidationTable, key)
				}
			}
		}
		delete(d.clientKeys, o)
	}
}

func (d *DistributedMap) Track(key string, o Observer) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 1. Add to invalidationTable
	if _, ok := d.invalidationTable[key]; !ok {
		d.invalidationTable[key] = make(map[Observer]struct{})
	}
	d.invalidationTable[key][o] = struct{}{}

	// 2. Add to clientKeys (for cleanup)
	if _, ok := d.clientKeys[o]; !ok {
		d.clientKeys[o] = make(map[string]struct{})
	}
	d.clientKeys[o][key] = struct{}{}
}

func (d *DistributedMap) NotifyObservers(key string) {
	d.mu.Lock() // Must be Lock, not RLock, because we modify the map (one-shot)
	defer d.mu.Unlock()

	observers, ok := d.invalidationTable[key]
	if !ok {
		return
	}

	// Notify and remove (One-shot semantics)
	for o := range observers {
		o.Invalidate(key)
		// Clean up reverse index
		if keys, ok := d.clientKeys[o]; ok {
			delete(keys, key)
			// Don't delete empty clientKeys entry here to avoid map thrashing,
			// or do it if memory is concern.
		}
	}

	// Remove from invalidationTable
	delete(d.invalidationTable, key)
}

type PubSub struct {
	mu       sync.RWMutex
	channels map[string]map[int64]chan string // channel -> clientID -> messageChan
	nextID   int64
}

func NewPubSub() *PubSub {
	return &PubSub{
		channels: make(map[string]map[int64]chan string),
		nextID:   1,
	}
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
		shards:            shards,
		mask:              uint64(size - 1),
		seed:              maphash.MakeSeed(),
		pubsub:            NewPubSub(),
		invalidationTable: make(map[string]map[Observer]struct{}),
		clientKeys:        make(map[Observer]map[string]struct{}),
	}
}

func (d *DistributedMap) hash(key string) uint64 {
	var h maphash.Hash
	h.SetSeed(d.seed)
	h.WriteString(key)
	return h.Sum64()
}

func (d *DistributedMap) GetShardIndex(key string) int {
	return int(d.hash(key) & d.mask)
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
	d.NotifyObservers(key)
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
		d.NotifyObservers(key) // Notify on expiration
		return nil, false
	}

	return item.Value, true
}

func (d *DistributedMap) Del(key string) {
	shard := d.getShard(key)
	shard.Delete(key)
	d.NotifyObservers(key)
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
		d.NotifyObservers(key)
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
		d.NotifyObservers(key) // Notify on expiration
		return -2              // Key expired (and thus does not exist)
	}

	return ttl
}
