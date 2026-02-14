package ledis

import (
	"math/rand"
	"sync"
	"time"
)

const (
	ZSKIPLIST_MAXLEVEL = 32
	ZSKIPLIST_P        = 0.25
)

type zskiplistLevel struct {
	forward *zskiplistNode
	span    int64
}

type zskiplistNode struct {
	member   string
	score    float64
	backward *zskiplistNode
	level    []zskiplistLevel
}

type zskiplist struct {
	header, tail *zskiplistNode
	length       int64
	level        int
}

type ZSet struct {
	mu   sync.RWMutex
	dict map[string]float64
	zsl  *zskiplist
}

func zslCreateNode(level int, score float64, member string) *zskiplistNode {
	n := &zskiplistNode{
		score:  score,
		member: member,
		level:  make([]zskiplistLevel, level),
	}
	return n
}

func zslCreate() *zskiplist {
	zsl := &zskiplist{
		level:  1,
		length: 0,
		header: zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, ""),
	}
	return zsl
}

func zslRandomLevel() int {
	level := 1
	for float64(rand.Int31()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF) {
		level += 1
	}
	if level < ZSKIPLIST_MAXLEVEL {
		return level
	}
	return ZSKIPLIST_MAXLEVEL
}

func (zsl *zskiplist) insert(score float64, member string) *zskiplistNode {
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	rank := make([]int64, ZSKIPLIST_MAXLEVEL)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := zslRandomLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = zsl.length
		}
		zsl.level = level
	}

	x = zslCreateNode(level, score, member)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	x.backward = nil
	if update[0] != zsl.header {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}
	zsl.length++
	return x
}

func (zsl *zskiplist) deleteNode(x *zskiplistNode, update []*zskiplistNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

func (zsl *zskiplist) delete(score float64, member string) int {
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && score == x.score && x.member == member {
		zsl.deleteNode(x, update)
		return 1
	}
	return 0
}

func (zsl *zskiplist) getRank(score float64, member string) int64 {
	rank := int64(0)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.member <= member)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x.member == member {
			return rank
		}
	}
	return 0
}

func (zsl *zskiplist) updateScore(curScore float64, member string, newScore float64) *zskiplistNode {
	update := make([]*zskiplistNode, ZSKIPLIST_MAXLEVEL)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < curScore ||
				(x.level[i].forward.score == curScore && x.level[i].forward.member < member)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && curScore == x.score && x.member == member {
		if (x.backward == nil || x.backward.score < newScore || (x.backward.score == newScore && x.backward.member < member)) &&
			(x.level[0].forward == nil || x.level[0].forward.score > newScore || (x.level[0].forward.score == newScore && x.level[0].forward.member > member)) {
			x.score = newScore
			return x
		}
		zsl.deleteNode(x, update)
		newNode := zsl.insert(newScore, member)
		return newNode
	}
	return nil
}

func NewZSet() *ZSet {
	return &ZSet{
		dict: make(map[string]float64),
		zsl:  zslCreate(),
	}
}

// Helper to get or create ZSet
func (d *DistributedMap) getOrCreateZSet(key string) (*ZSet, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		z := NewZSet()
		val, loaded := shard.LoadOrStore(key, Item{Value: z, ExpiresAt: 0})
		if loaded {
			item := val.(Item)
			if zVal, ok := item.Value.(*ZSet); ok {
				return zVal, nil
			}
			return nil, ErrWrongType
		}
		return z, nil
	}
	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		z := NewZSet()
		shard.Store(key, Item{Value: z, ExpiresAt: 0})
		return z, nil
	}
	z, ok := item.Value.(*ZSet)
	if !ok {
		return nil, ErrWrongType
	}
	return z, nil
}

// Commands

func (d *DistributedMap) ZAdd(key string, score float64, member string) (int, error) {
	z, err := d.getOrCreateZSet(key)
	if err != nil {
		return 0, err
	}

	z.mu.Lock()
	defer z.mu.Unlock()

	added := 0
	if oldScore, ok := z.dict[member]; ok {
		if oldScore != score {
			z.zsl.updateScore(oldScore, member, score)
			z.dict[member] = score
		}
	} else {
		z.zsl.insert(score, member)
		z.dict[member] = score
		added = 1
	}
	return added, nil
}

func (d *DistributedMap) ZRem(key string, members ...string) (int, error) {
	// Need getSet
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return 0, nil
	}
	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		d.Del(key)
		return 0, nil
	}
	z, ok := item.Value.(*ZSet)
	if !ok {
		return 0, ErrWrongType
	}

	z.mu.Lock()
	defer z.mu.Unlock()

	removed := 0
	for _, m := range members {
		if score, ok := z.dict[m]; ok {
			z.zsl.delete(score, m)
			delete(z.dict, m)
			removed++
		}
	}

	if len(z.dict) == 0 {
		d.Del(key)
	}
	return removed, nil
}

func (d *DistributedMap) ZScore(key string, member string) (float64, bool, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return 0, false, nil
	}
	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		d.Del(key)
		return 0, false, nil
	}
	z, ok := item.Value.(*ZSet)
	if !ok {
		return 0, false, ErrWrongType
	}

	z.mu.RLock()
	defer z.mu.RUnlock()

	score, exists := z.dict[member]
	return score, exists, nil
}

func (d *DistributedMap) ZCard(key string) (int64, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return 0, nil
	}
	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		d.Del(key)
		return 0, nil
	}
	z, ok := item.Value.(*ZSet)
	if !ok {
		return 0, ErrWrongType
	}

	z.mu.RLock()
	defer z.mu.RUnlock()

	return z.zsl.length, nil
}

func (d *DistributedMap) ZIncrBy(key string, increment float64, member string) (float64, error) {
	z, err := d.getOrCreateZSet(key)
	if err != nil {
		return 0, err
	}

	z.mu.Lock()
	defer z.mu.Unlock()

	score := increment
	if oldScore, ok := z.dict[member]; ok {
		score = oldScore + increment
		z.zsl.updateScore(oldScore, member, score)
	} else {
		z.zsl.insert(score, member)
	}
	z.dict[member] = score
	return score, nil
}

type ZRangeResult struct {
	Member string
	Score  float64
}

func (d *DistributedMap) ZRange(key string, start, stop int64, withScores bool) ([]interface{}, error) {
	return d.zrangeGeneric(key, start, stop, withScores, false)
}

func (d *DistributedMap) ZRevRange(key string, start, stop int64, withScores bool) ([]interface{}, error) {
	return d.zrangeGeneric(key, start, stop, withScores, true)
}

func (d *DistributedMap) zrangeGeneric(key string, start, stop int64, withScores, reverse bool) ([]interface{}, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		return []interface{}{}, nil
	}
	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		d.Del(key)
		return []interface{}{}, nil
	}
	z, ok := item.Value.(*ZSet)
	if !ok {
		return nil, ErrWrongType
	}

	z.mu.RLock()
	defer z.mu.RUnlock()

	length := z.zsl.length
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop = length + stop
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return []interface{}{}, nil
	}

	rangeLen := stop - start + 1
	result := make([]interface{}, 0, rangeLen) // If withScores, doubling happens in fetch? No, interface slice.

	// Traverse
	var x *zskiplistNode
	if reverse {
		// Start from tail? Or find by rank?
		// We don't have GetNodeByRank. We must implement it or traverse.
		// Implementing GetNodeByRank (O(logN)) is better than traversing O(N).
		// But for now, let's implement naive traversal for simplicity or full implementation.
		// Wait, start rank is known.
		x = z.zsl.getNodeByRank(uint64(length - start))
	} else {
		x = z.zsl.getNodeByRank(uint64(start + 1))
	}

	for i := int64(0); i < rangeLen; i++ {
		if x == nil {
			break
		}
		if withScores {
			result = append(result, x.member, x.score) // Redis returns flat list
		} else {
			result = append(result, x.member)
		}
		if reverse {
			x = x.backward
		} else {
			x = x.level[0].forward
		}
	}
	return result, nil
}

func (zsl *zskiplist) getNodeByRank(rank uint64) *zskiplistNode {
	x := zsl.header
	traversed := uint64(0)
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+uint64(x.level[i].span) <= rank {
			traversed += uint64(x.level[i].span)
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}
