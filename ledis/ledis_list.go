package ledis

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoSuchKey = errors.New("ERR no such key")
	ErrTimeout   = errors.New("ERR timeout")
)

// Helper to get list item if exists
func (d *DistributedMap) getListItem(key string) (*Item, error) {
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

	if item.Type != TypeList {
		return nil, ErrWrongType
	}
	return item, nil
}

// Helper to get or create a list item
func (d *DistributedMap) getOrCreateListItem(key string) (*Item, error) {
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
			if item.Type != TypeList {
				return nil, ErrWrongType
			}
			return item, nil
		}
	}

	// Create new
	newItem := itemPool.Get().(*Item)
	newItem.reset()
	newItem.Type = TypeList
	newItem.List = make([]string, 0)
	newItem.Waiters = make([]chan string, 0)
	newItem.ExpiresAt = 0

	actual, loaded := shard.LoadOrStore(key, newItem)
	if loaded {
		newItem.reset()
		itemPool.Put(newItem)

		item := actual.(*Item)
		if item.ExpiresAt > 0 && item.ExpiresAt < time.Now().UnixNano() {
			return d.getOrCreateListItem(key)
		}
		if item.Type != TypeList {
			return nil, ErrWrongType
		}
		return item, nil
	}

	d.NotifyObservers(key)
	return newItem, nil
}

// Internal push helpers on *Item
func (i *Item) rpush(values ...string) int {
	i.Mu.Lock()
	defer i.Mu.Unlock()

	// Satisfy waiters
	for len(i.Waiters) > 0 && len(values) > 0 {
		ch := i.Waiters[0]
		i.Waiters = i.Waiters[1:]
		val := values[0]
		values = values[1:]

		select {
		case ch <- val:
		default:
		}
	}

	if len(values) > 0 {
		i.List = append(i.List, values...)
	}
	return len(i.List)
}

func (i *Item) lpush(values ...string) int {
	i.Mu.Lock()
	defer i.Mu.Unlock()

	// Waiters logic for LPush (serve head/tail?)
	// If LPUSH a b. List: [b, a].
	// Waiters get 'a' then 'b'?
	// Actually LPUSH pushes to head. If empty, pushed val is available.
	// If multiple values pushed, they are pushed in order?
	// Redis: LPUSH mylist a b c => c, b, a.
	// We should feed waiters from the values that *would* be at head.
	// My LPush wrapper reverses values before calling.
	// So [c, b, a] passed here. 'c' is head.
	// Waiters get 'c', then 'b', then 'a'.

	for len(i.Waiters) > 0 && len(values) > 0 {
		ch := i.Waiters[0]
		i.Waiters = i.Waiters[1:]
		val := values[0]
		values = values[1:]

		select {
		case ch <- val:
		default:
		}
	}

	if len(values) > 0 {
		i.List = append(values, i.List...)
	}
	return len(i.List)
}

func (i *Item) pop(left bool) (string, bool) {
	i.Mu.Lock()
	defer i.Mu.Unlock()
	if len(i.List) == 0 {
		return "", false
	}
	var val string
	if left {
		val = i.List[0]
		i.List = i.List[1:]
	} else {
		val = i.List[len(i.List)-1]
		i.List = i.List[:len(i.List)-1]
	}
	return val, true
}

// Commands

func (d *DistributedMap) LPush(key string, values ...interface{}) (int, error) {
	// Convert values to strings
	strValues := make([]string, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case string:
			strValues[i] = val
		default:
			strValues[i] = fmt.Sprintf("%v", val)
		}
	}

	item, err := d.getOrCreateListItem(key)
	if err != nil {
		return 0, err
	}

	// Reverse for LPUSH semantics
	for i, j := 0, len(strValues)-1; i < j; i, j = i+1, j-1 {
		strValues[i], strValues[j] = strValues[j], strValues[i]
	}

	return item.lpush(strValues...), nil
}

func (d *DistributedMap) RPush(key string, values ...interface{}) (int, error) {
	strValues := make([]string, len(values))
	for i, v := range values {
		switch val := v.(type) {
		case string:
			strValues[i] = val
		default:
			strValues[i] = fmt.Sprintf("%v", val)
		}
	}

	item, err := d.getOrCreateListItem(key)
	if err != nil {
		return 0, err
	}
	return item.rpush(strValues...), nil
}

func (d *DistributedMap) LPop(key string) (interface{}, error) {
	item, err := d.getListItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	val, ok := item.pop(true)
	if !ok {
		return nil, nil
	}

	// Check if empty after pop
	item.Mu.RLock()
	isEmpty := len(item.List) == 0
	item.Mu.RUnlock()

	if isEmpty {
		d.Del(key)
	}
	return val, nil
}

func (d *DistributedMap) RPop(key string) (interface{}, error) {
	item, err := d.getListItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}
	val, ok := item.pop(false)
	if !ok {
		return nil, nil
	}

	item.Mu.RLock()
	isEmpty := len(item.List) == 0
	item.Mu.RUnlock()

	if isEmpty {
		d.Del(key)
	}
	return val, nil
}

func (d *DistributedMap) blockPop(key string, timeout time.Duration, left bool) (interface{}, error) {
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

	// Register waiter
	item, err := d.getOrCreateListItem(key)
	if err != nil {
		return nil, err
	}

	ch := make(chan string, 1)

	item.Mu.Lock()
	if len(item.List) > 0 {
		item.Mu.Unlock()
		if left {
			return d.LPop(key)
		} else {
			return d.RPop(key)
		}
	}
	item.Waiters = append(item.Waiters, ch)
	item.Mu.Unlock()

	// Wait
	select {
	case v := <-ch:
		return v, nil
	case <-time.After(timeout):
		item.Mu.Lock()
		defer item.Mu.Unlock()
		for i, c := range item.Waiters {
			if c == ch {
				item.Waiters = append(item.Waiters[:i], item.Waiters[i+1:]...)
				break
			}
		}
		return nil, ErrTimeout
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
	item, err := d.getListItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}
	item.Mu.RLock()
	defer item.Mu.RUnlock()
	return len(item.List), nil
}

func (d *DistributedMap) LRange(key string, start, stop int) ([]interface{}, error) {
	item, err := d.getListItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []interface{}{}, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	length := len(item.List)
	if length == 0 {
		return []interface{}{}, nil
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
		return []interface{}{}, nil
	}

	// Copy result as interface{} for compatibility
	result := make([]interface{}, stop-start+1)
	for i, v := range item.List[start : stop+1] {
		result[i] = v
	}
	return result, nil
}

func (d *DistributedMap) LIndex(key string, index int) (interface{}, error) {
	item, err := d.getListItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	length := len(item.List)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return nil, nil
	}

	return item.List[index], nil
}

func (d *DistributedMap) LSet(key string, index int, value interface{}) error {
	item, err := d.getListItem(key)
	if err != nil {
		return err
	}
	if item == nil {
		return ErrNoSuchKey
	}

	strVal := ""
	switch v := value.(type) {
	case string:
		strVal = v
	default:
		strVal = fmt.Sprintf("%v", v)
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	length := len(item.List)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return errors.New("ERR index out of range")
	}

	item.List[index] = strVal
	return nil
}

func (d *DistributedMap) LTrim(key string, start, stop int) error {
	item, err := d.getListItem(key)
	if err != nil {
		return err
	}
	if item == nil {
		return nil
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	length := len(item.List)
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
		item.List = make([]string, 0)
		item.Mu.Unlock()
		d.Del(key)
		return nil
	}

	item.List = item.List[start : stop+1]
	return nil
}

func (d *DistributedMap) LRem(key string, count int, value interface{}) (int, error) {
	strVal := ""
	switch v := value.(type) {
	case string:
		strVal = v
	default:
		strVal = fmt.Sprintf("%v", v)
	}

	item, err := d.getListItem(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, nil
	}
	item.Mu.Lock()

	removed := 0
	newData := make([]string, 0, len(item.List))

	if count > 0 {
		for _, v := range item.List {
			if v == strVal && removed < count {
				removed++
				continue
			}
			newData = append(newData, v)
		}
	} else if count < 0 {
		count = -count
		temp := make([]string, 0, len(item.List))
		for i := len(item.List) - 1; i >= 0; i-- {
			v := item.List[i]
			if v == strVal && removed < count {
				removed++
				continue
			}
			temp = append(temp, v)
		}
		for i := len(temp) - 1; i >= 0; i-- {
			newData = append(newData, temp[i])
		}
	} else {
		for _, v := range item.List {
			if v != strVal {
				newData = append(newData, v)
			} else {
				removed++
			}
		}
	}

	item.List = newData

	isEmpty := len(item.List) == 0
	item.Mu.Unlock()

	if isEmpty {
		d.Del(key)
	}

	return removed, nil
}
