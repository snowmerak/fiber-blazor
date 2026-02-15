package ledis

import (
	"context"
	"fmt" // Added for fmt.Sprintf in Set method
	"hash/maphash"
	"math/bits"
	"runtime"
	"strconv" // Added for toInt64
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/panjf2000/ants/v2"
)

const (
	evictSampleRate = 20
	evictThreshold  = 5 // 25% of 20
)

const (
	TypeString = iota
	TypeList
	TypeHash
	TypeSet
	TypeZSet
	TypeStream
	TypeBitmap
)

type ListNode struct {
	Value string
	Prev  *ListNode
	Next  *ListNode
}

type Item struct {
	Type      uint8
	ExpiresAt int64
	Mu        sync.RWMutex // Protects mutable fields

	// Value holders - Concrete types to avoid interface{} boxing
	Str      string
	ListHead *ListNode
	ListTail *ListNode
	ListSize int
	Hash     map[string]string
	Set      map[string]struct{}
	ZSet     *SortedSet
	Bitmap   *roaring64.Bitmap
	Stream   *Stream

	// Waiters for blocking list operations
	Waiters []chan string
}

func (i *Item) reset() {
	i.Type = 0
	i.ExpiresAt = 0
	// Mu state is not reset, but if we reuse, we assume no one holds lock
	i.Str = ""
	i.ListHead = nil
	i.ListTail = nil
	i.ListSize = 0
	i.Hash = nil
	i.Set = nil
	i.ZSet = nil
	i.Bitmap = nil
	i.Stream = nil
	i.Waiters = nil
}

// Helper to convert numeric types to int64
func toInt64(val any) (int64, error) {
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
		return int64(v), nil
	case string:
		// Attempt parse
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("value is not an integer")
		}
		return i, nil
	default:
		return 0, fmt.Errorf("value is not an integer")
	}
}

var itemPool = sync.Pool{
	New: func() any {
		return &Item{}
	},
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

	WorkerPool *ants.Pool

	// Eviction
	evictCtx    context.Context
	evictCancel context.CancelFunc
	wg          sync.WaitGroup
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

	pool, _ := ants.NewPool(runtime.NumCPU() * 4)

	d := &DistributedMap{
		shards:            shards,
		mask:              uint64(size - 1),
		seed:              maphash.MakeSeed(),
		pubsub:            NewPubSub(),
		invalidationTable: make(map[string]map[Observer]struct{}),
		clientKeys:        make(map[Observer]map[string]struct{}),
		WorkerPool:        pool,
	}

	d.evictCtx, d.evictCancel = context.WithCancel(context.Background())
	d.wg.Add(1)
	go d.startEvictLoop()

	return d
}

func (d *DistributedMap) hash(key string) uint64 {
	var h maphash.Hash
	h.SetSeed(d.seed)
	h.WriteString(key)
	return h.Sum64()
}

func (d *DistributedMap) Close() {
	d.evictCancel()
	d.wg.Wait()
	d.WorkerPool.Release()
}

func (d *DistributedMap) startEvictLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Strategy 2 state
	currentShard := 0
	lastScanTime := time.Time{} // Zero time means ready to scan if not in rest period
	scanningActive := true      // Start active

	for {
		select {
		case <-d.evictCtx.Done():
			return
		case <-ticker.C:
			// Strategy 1: Probabilistic Sampling on ALL shards (or subset?)
			// Redis does it on all DBs. We have 1024 shards.
			// Sampling 20 keys from 1024 shards every second = 20k lookups.
			// Might be too heavy?
			// Redis by default is 10 times per second.
			// If we do 1s interval, maybe we sample a subset of shards?
			// Or just rely on Strategy 2 mainly and use Strategy 1 on "active" shards?
			// Let's iterate all shards for Strategy 1 as planned, but maybe limit N?
			// 20 keys * 1024 shards = 20480 checks/sec.
			// Map lookup is ~50ns. 20k * 50ns = 1ms.
			// 1ms CPU time per second is negligible.
			d.evictSample()

			// Strategy 2: Background Scan
			if scanningActive {
				// Scan 1 shard
				d.evictScanShard(currentShard)
				currentShard++
				if currentShard >= len(d.shards) {
					// Cycle complete
					currentShard = 0
					scanningActive = false
					lastScanTime = time.Now()
				}
			} else {
				// Check if rest period is over
				if time.Since(lastScanTime) >= 5*time.Minute {
					scanningActive = true
				}
			}
		}
	}
}

func (d *DistributedMap) evictSample() {
	for _, shard := range d.shards {
		// sync.Map doesn't have random access or length suitable for random sampling.
		// range is the only way, but range iterates all.
		// We can't efficienty sample random keys from sync.Map without keys list.
		// HACK: We use Range and stop after N items.
		// This always checks the *same* N items if map structure doesn't change!
		// Redis uses a hash table where it can pick random bucket.
		// sync.Map is complex.
		// If we can't do random, "First N" is bad (biased).
		// Alternative: We skip distinct amount? No efficient way.
		//
		// Given sync.Map limitation, Strategy 1 (Random Sampling) is hard to implement correctly/efficiently.
		// Strategy 2 (Full Scan) is feasible.
		//
		// compromise: We rely heavily on Strategy 2.
		// Or, since we are doing Strategy 2 (Sequential Scan), maybe that's enough?
		// User accepted "Hybrid".
		// To make "Sample" vaguely random in Range:
		// We can't.
		//
		// Wait, if we use just Strategy 2, we cover everything.
		// Let's implement evictSample as "Scan a few random shards fully"?
		// No.
		//
		// Let's implement Strategy 2 ROBUSTLY and maybe skip Strategy 1 if it's ineffective on sync.Map?
		// Or, use `Range` but abort after 20 items.
		// It's better than nothing, effectively "Scanning head".
		// If keys are inserted/deleted, "head" changes.

		expiredCount := 0
		checked := 0

		shard.Range(func(key, value any) bool {
			checked++
			item := value.(*Item)
			if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
				// Use LoadAndDelete to ensure thread safety
				if _, ok := shard.LoadAndDelete(key); ok {
					d.NotifyObservers(key.(string))
					expiredCount++
				}
			}

			return checked < evictSampleRate
		})
	}
}

func (d *DistributedMap) evictScanShard(shardIdx int) {
	if shardIdx < 0 || shardIdx >= len(d.shards) {
		return
	}
	shard := d.shards[shardIdx]

	// Scan ALL items in this shard
	shard.Range(func(key, value any) bool {
		item := value.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			if _, ok := shard.LoadAndDelete(key); ok {
				d.NotifyObservers(key.(string))
			}
		}
		// Continue iteration
		return true
	})
}

func (d *DistributedMap) GetShardIndex(key string) int {
	return int(d.hash(key) & d.mask)
}

func (d *DistributedMap) getShard(key string) *sync.Map {
	idx := d.hash(key) & d.mask
	return d.shards[idx]
}

// Set is now internal helper or specific type setter?
// No, Set is usually for String type in Redis.
// But here Set was generic.
// We need to change Set to SetString or update it to handle generic value if passed?
// The original Set took interface{}.
// To support backward compatibility or easy refactor, checking type of value is needed?
// Actually, generic Set is rarely used if we have SetString, SetList etc.
// But lets keep it for String mostly or refactor `ledis_string.go` to use `setItem`.
// For now, let's implement `setItem` helper and generic Set acting as String Set?
// Or better, update Set to take string value?
// The interface `Set(key string, value interface{}, duration time.Duration)` suggests generic.
// If I change `Item`, `Set` must convert `value` to appropriate field.
// This is messy if we keep `Set` generic.
// Better to deprecate `Set` and use `SetString`, `SetList` etc.
// But for this file, let's update `Set` to handle String only or panic/error?
// Or type switch.

func (d *DistributedMap) Set(key string, value any, duration time.Duration) {
	// Assumes String for generic Set, or type switch
	// Ideally we refactor usages.
	// For now, support string only or basic types

	item := itemPool.Get().(*Item)
	item.reset() // Ensure clean state

	item.ExpiresAt = 0
	if duration > 0 {
		item.ExpiresAt = time.Now().Add(duration).UnixNano()
	}

	switch v := value.(type) {
	case string:
		item.Type = TypeString
		item.Str = v
	case []string:
		item.Type = TypeList
		for _, s := range v {
			node := &ListNode{Value: s}
			if item.ListHead == nil {
				item.ListHead = node
				item.ListTail = node
			} else {
				item.ListTail.Next = node
				node.Prev = item.ListTail
				item.ListTail = node
			}
			item.ListSize++
		}
	case map[string]string:
		item.Type = TypeHash
		item.Hash = v
	case map[string]struct{}:
		item.Type = TypeSet
		item.Set = v
	default:
		// Fallback or error? For now assume string if unknown or let it fail?
		// Previous impl stored anything.
		// If we want to support any interface{}, we can't with this Item struct easily.
		// But existing code mostly uses primitives.
		// We'll trust callers pass supported types or we add `Any interface{}` field?
		// No, that defeats the purpose.
		// Let's assume String for now as default fallthrough if simple Set is called.
		item.Type = TypeString
		item.Str = fmt.Sprintf("%v", v)
	}

	shard := d.getShard(key)
	// Check existing to release to pool?
	if old, ok := shard.Load(key); ok {
		if _, ok := old.(*Item); ok {
			// oldItem.reset()
			// itemPool.Put(oldItem)
		}
	}

	shard.Store(key, item)
	d.NotifyObservers(key)
}

// Get returns the raw *Item for the given key.
// Caller MUST Lock/RLock item.Mu before accessing fields.
func (d *DistributedMap) Get(key string) (*Item, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return nil, ErrNoSuchKey
	}

	item := val.(*Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		shard.Delete(key)
		d.NotifyObservers(key)
		// item.reset()
		// itemPool.Put(item)
		return nil, ErrNoSuchKey
	}

	return item, nil
}

func (d *DistributedMap) Del(key string) {
	shard := d.getShard(key)
	if val, ok := shard.LoadAndDelete(key); ok {
		if _, ok := val.(*Item); ok {
			// item.reset()
			// itemPool.Put(item)
		}
		d.NotifyObservers(key)
	}
}

func (d *DistributedMap) Exists(key string) bool {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return false
	}

	item := val.(*Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		shard.Delete(key)
		d.NotifyObservers(key)
		// item.reset()
		// itemPool.Put(item)
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

	item := val.(*Item)
	if item.ExpiresAt == 0 {
		return -1 // No expiration
	}

	ttl := time.Duration(item.ExpiresAt - time.Now().UnixNano())
	if ttl < 0 {
		shard.Delete(key)
		d.NotifyObservers(key) // Notify on expiration
		// item.reset()
		// itemPool.Put(item)
		return -2 // Key expired
	}

	return ttl
}
