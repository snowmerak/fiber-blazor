package ledis

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoSuchKey = errors.New("ERR no such key")
	ErrTimeout   = errors.New("ERR timeout")
)

type List struct {
	mu      sync.RWMutex
	Data    []interface{}
	Waiters []chan interface{} // Channels for blocking clients
}

func NewList() *List {
	return &List{
		Data:    make([]interface{}, 0),
		Waiters: make([]chan interface{}, 0),
	}
}

func (l *List) rpush(values ...interface{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If there are waiters, satisfy them first
	for len(l.Waiters) > 0 && len(values) > 0 {
		ch := l.Waiters[0]
		l.Waiters = l.Waiters[1:]
		val := values[0]
		values = values[1:]

		// Send non-blocking if possible, or blocking?
		// Since waiter is waiting on select, it should be ready.
		// Use strict send.
		select {
		case ch <- val:
		default:
			// If channel full/closed (timeout?), just drop?
			// But we removed it from Waiters.
			// Ideally we assume waiter is active or we handle 'closed' waiters structure.
			// For simplicity: buffered channel size 1?
			// If we can't send, it means waiter timed out or gone.
		}
	}

	if len(values) > 0 {
		l.Data = append(l.Data, values...)
	}
	return len(l.Data)
}

func (l *List) lpush(values ...interface{}) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If there are waiters, satisfy them first
	// Redis BLPOP/BRPOP: If list empty, block.
	// When RPUSH/LPUSH happens, element serves waiter.
	// Does direction matter?
	// BLPOP: pop from head. LPush: push to head.
	// If I push 'a', 'b'. 'a' is at tailmost of push batch if pushed first?
	// Redis: LPUSH key a b c -> [c, b, a].
	// Waiters are served FCFS.
	// We feed waiters from `values`?
	// If I LPUSH a b c. List empty. 1 waiter.
	// Waiter gets 'c'? (Last pushed becomes head).
	// Actually Redis varies. Assuming we feed waiters with what would have been head.
	// In `LPush`, `values` are reversed before call in my implementation of `LPush` wrapper?
	// No, inside `lpush` struct method, I did `append(values, l.Data...)`.
	// My `LPush` wrapper reversed them: `LPUSH key a b c` -> `values` becomes `[c, b, a]`.
	// `append([c, b, a], data...)`. `c` is at index 0 (Head).
	// So `c` is the first element available for `LPop` (Head).
	// So we should feed `c` to waiter?
	// Yes. `c` is at `values[0]`.

	for len(l.Waiters) > 0 && len(values) > 0 {
		ch := l.Waiters[0]
		l.Waiters = l.Waiters[1:]
		val := values[0]
		values = values[1:]

		select {
		case ch <- val:
		default:
		}
	}

	if len(values) > 0 {
		l.Data = append(values, l.Data...)
	}
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

// Blocking Commands

func (d *DistributedMap) blockPop(key string, timeout time.Duration, left bool) (interface{}, error) {
	// First try non-blocking pop
	// We need to loop because we might be woken up but data stolen, or logic needs retry?
	// But `lpush` hands data directly to us via channel.

	var val interface{}
	var err error

	if left {
		val, err = d.LPop(key)
	} else {
		val, err = d.RPop(key)
	}

	if err != nil {
		return nil, err
	}
	if val != nil {
		return val, nil
	}

	// List empty or missing. Register waiter.
	l, err := d.getOrCreateList(key) // Must create if not exists to register waiter?
	// Redis BLPOP keys... if key not exists, it treats as empty list and blocks.
	if err != nil {
		return nil, err
	}

	ch := make(chan interface{}, 1)

	l.mu.Lock()
	// Re-check just in case
	if len(l.Data) > 0 {
		l.mu.Unlock()
		if left {
			return d.LPop(key)
		} else {
			return d.RPop(key)
		}
	}
	l.Waiters = append(l.Waiters, ch)
	l.mu.Unlock()

	// Wait
	select {
	case v := <-ch:
		return v, nil
	case <-time.After(timeout):
		// Unregister waiter
		l.mu.Lock()
		defer l.mu.Unlock()
		// Find and remove ch
		for i, c := range l.Waiters {
			if c == ch {
				l.Waiters = append(l.Waiters[:i], l.Waiters[i+1:]...)
				break
			}
		}
		return nil, ErrTimeout // Or return nil, nil? Redis returns nil on timeout.
	}
}

func (d *DistributedMap) BLPop(key string, timeout time.Duration) (interface{}, error) {
	return d.blockPop(key, timeout, true)
}

func (d *DistributedMap) BRPop(key string, timeout time.Duration) (interface{}, error) {
	return d.blockPop(key, timeout, false)
}

// PushX Variants

func (d *DistributedMap) LPushX(key string, values ...interface{}) (int, error) {
	exists := d.Exists(key)
	if !exists {
		return 0, nil
	}
	return d.LPush(key, values...)
}

func (d *DistributedMap) RPushX(key string, values ...interface{}) (int, error) {
	exists := d.Exists(key)
	if !exists {
		return 0, nil
	}
	return d.RPush(key, values...)
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
		return nil, nil
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
		l.Data = make([]interface{}, 0)
		d.Del(key)
		return nil
	}

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
		temp := make([]interface{}, 0, len(l.Data))
		for i := len(l.Data) - 1; i >= 0; i-- {
			v := l.Data[i]
			if v == value && removed < count {
				removed++
				continue
			}
			temp = append(temp, v)
		}
		for i := len(temp) - 1; i >= 0; i-- {
			newData = append(newData, temp[i])
		}
	} else {
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
