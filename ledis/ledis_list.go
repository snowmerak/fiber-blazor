package ledis

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoSuchKey = errors.New("ERR no such key")
)

type List struct {
	mu   sync.RWMutex
	Data []interface{}
}

func NewList() *List {
	return &List{
		Data: make([]interface{}, 0),
	}
}

func (l *List) rpush(values ...interface{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Data = append(l.Data, values...)
	return len(l.Data)
}

func (l *List) lpush(values ...interface{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Prepend
	l.Data = append(values, l.Data...)
	return len(l.Data)
}

func (l *List) pop(left bool) (interface{}, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.Data) == 0 {
		return nil, false
	}
	var val interface{}
	if left {
		val = l.Data[0]
		l.Data = l.Data[1:]
	} else {
		val = l.Data[len(l.Data)-1]
		l.Data = l.Data[:len(l.Data)-1]
	}
	return val, true
}

func (l *List) len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.Data)
}

func (l *List) rangeList(start, stop int) []interface{} {
	l.mu.RLock()
	defer l.mu.RUnlock()
	length := len(l.Data)
	if length == 0 {
		return []interface{}{}
	}

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
		return []interface{}{}
	}

	// Return copy to avoid race on slice underlying array if modified later?
	// Yes, must copy.
	result := make([]interface{}, stop-start+1)
	copy(result, l.Data[start:stop+1])
	return result
}

// Helper to get or create a list
func (d *DistributedMap) getOrCreateList(key string) (*List, error) {
	shard := d.getShard(key)
	val, ok := shard.Load(key)
	if !ok {
		l := NewList()
		val, loaded := shard.LoadOrStore(key, Item{Value: l, ExpiresAt: 0})
		if loaded {
			// Check type if race loaded it
			item := val.(Item)
			if lVal, ok := item.Value.(*List); ok {
				return lVal, nil
			}
			return nil, ErrWrongType
		}
		return l, nil
	}

	item := val.(Item)
	if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
		// Expired, replace
		l := NewList()
		shard.Store(key, Item{Value: l, ExpiresAt: 0})
		return l, nil
	}

	l, ok := item.Value.(*List)
	if !ok {
		return nil, ErrWrongType
	}
	return l, nil
}

// Helper to get list if exists
func (d *DistributedMap) getList(key string) (*List, error) {
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

	l, ok := item.Value.(*List)
	if !ok {
		return nil, ErrWrongType
	}
	return l, nil
}

// Commands

func (d *DistributedMap) LPush(key string, values ...interface{}) (int, error) {
	l, err := d.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	// Note: Redis LPUSH inserts in reverse order of arguments if multiple?
	// "LPUSH mylist a b c" -> c, b, a
	// My internal lpush processes `values` as a block.
	// append(values, l.Data...) puts values as [a, b, c, old...].
	// This means 'a' is at index 0.
	// Redis: LPUSH mylist a b c => list is [c, b, a].
	// So I should reverse `values` before calling `lpush` if I want strict Redis compatibility.
	// Let's reverse them here.
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
	return l.lpush(values...), nil
}

func (d *DistributedMap) RPush(key string, values ...interface{}) (int, error) {
	l, err := d.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	return l.rpush(values...), nil
}

func (d *DistributedMap) LPop(key string) (interface{}, error) {
	l, err := d.getList(key)
	if err != nil {
		return nil, err
	}
	if l == nil {
		return nil, nil
	}
	val, ok := l.pop(true)
	if !ok {
		// List empty, should we remove key? Redis does remove empty list keys.
		d.Del(key)
		return nil, nil
	}
	if l.len() == 0 {
		d.Del(key)
	}
	return val, nil
}

func (d *DistributedMap) RPop(key string) (interface{}, error) {
	l, err := d.getList(key)
	if err != nil {
		return nil, err
	}
	if l == nil {
		return nil, nil
	}
	val, ok := l.pop(false)
	if !ok {
		d.Del(key)
		return nil, nil
	}
	if l.len() == 0 {
		d.Del(key)
	}
	return val, nil
}

func (d *DistributedMap) LLen(key string) (int, error) {
	l, err := d.getList(key)
	if err != nil {
		return 0, err
	}
	if l == nil {
		return 0, nil
	}
	return l.len(), nil
}

func (d *DistributedMap) LRange(key string, start, stop int) ([]interface{}, error) {
	l, err := d.getList(key)
	if err != nil {
		return nil, err
	}
	if l == nil {
		return []interface{}{}, nil
	}
	return l.rangeList(start, stop), nil
}

func (d *DistributedMap) LIndex(key string, index int) (interface{}, error) {
	l, err := d.getList(key)
	if err != nil {
		return nil, err
	}
	if l == nil {
		return nil, nil
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	length := len(l.Data)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return nil, nil // Redis returns nil for out of range
	}

	return l.Data[index], nil
}

func (d *DistributedMap) LSet(key string, index int, value interface{}) error {
	l, err := d.getList(key)
	if err != nil {
		return err
	}
	if l == nil {
		return ErrNoSuchKey
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	length := len(l.Data)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return errors.New("ERR index out of range")
	}

	l.Data[index] = value
	return nil
}

func (d *DistributedMap) LTrim(key string, start, stop int) error {
	l, err := d.getList(key)
	if err != nil {
		return err
	}
	if l == nil {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	length := len(l.Data)
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
		// Empty list
		l.Data = make([]interface{}, 0)
		// Should we delete key? Redis LTRIM does not delete key if empty?
		// Actually typical Redis behavior is to remove empty keys, but LTRIM result might be an empty list which persists?
		// Checked: Redis LTRIM that empties the list DOES NOT remove the key immediately?
		// "IF the result is empty list, the key is removed".
		// Let's implement key removal if empty.
		d.Del(key) // This is dangerous inside lock? No, Del locks shard, we locked List.
		// Wait, `d.Del` locks `shard`. `d.getList` locked `shard` briefly then returned.
		// `l.mu` is locked. `d.Del` locks `shard`. Safe order?
		// `getList` locks `shard` but unlocks before returning.
		// So `l.mu` is held. `d.Del` locks `shard`.
		// Are there cases where we hold `shard` lock and acquire `l.mu`?
		// `getOrCreateList` locks `shard` then `l.mu`? No, `NewList` doesn't lock.
		// We don't seem to lock `l.mu` inside `shard` lock. `shard` lock protects `Item`. `l.mu` protects `List` content.
		// So `d.Del(key)` is safe here.
		return nil
	}

	// Slice list
	l.Data = l.Data[start : stop+1]
	return nil
}

func (d *DistributedMap) LRem(key string, count int, value interface{}) (int, error) {
	l, err := d.getList(key)
	if err != nil {
		return 0, err
	}
	if l == nil {
		return 0, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	removed := 0
	// To perform LRem with generic value, we need comparison.
	// `value` is interface{}. strict equality `==`.
	// count > 0: Remove elements equal to value moving from head to tail.
	// count < 0: Remove elements equal to value moving from tail to head.
	// count = 0: Remove all elements equal to value.

	newData := make([]interface{}, 0, len(l.Data))

	if count > 0 {
		for _, v := range l.Data {
			if v == value && removed < count {
				removed++
				continue
			}
			newData = append(newData, v)
		}
	} else if count < 0 {
		count = -count
		// Traverse backwards to mark indices to remove?
		// Or iterate backwards, build new list backwards?
		// Let's iterate backwards.
		temp := make([]interface{}, 0, len(l.Data))
		for i := len(l.Data) - 1; i >= 0; i-- {
			v := l.Data[i]
			if v == value && removed < count {
				removed++
				continue
			}
			temp = append(temp, v)
		}
		// Reverse temp to get newData
		for i := len(temp) - 1; i >= 0; i-- {
			newData = append(newData, temp[i])
		}
	} else {
		// count == 0, remove all
		for _, v := range l.Data {
			if v != value {
				newData = append(newData, v)
			} else {
				removed++
			}
		}
	}

	l.Data = newData
	if len(l.Data) == 0 {
		d.Del(key)
	}

	return removed, nil
}
