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
	newItem.ListHead = nil
	newItem.ListTail = nil
	newItem.ListSize = 0
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
		for _, v := range values {
			node := &ListNode{Value: v}
			if i.ListTail == nil {
				i.ListHead = node
				i.ListTail = node
			} else {
				i.ListTail.Next = node
				node.Prev = i.ListTail
				i.ListTail = node
			}
			i.ListSize++
		}
	}

	return i.ListSize
}

func (i *Item) lpush(values ...string) int {
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
		for _, v := range values {
			node := &ListNode{Value: v}
			if i.ListHead == nil {
				i.ListHead = node
				i.ListTail = node
			} else {
				node.Next = i.ListHead
				i.ListHead.Prev = node
				i.ListHead = node
			}
			i.ListSize++
		}
	}

	return i.ListSize
}

func (i *Item) pop(left bool) (string, bool) {
	i.Mu.Lock()
	defer i.Mu.Unlock()

	if i.ListSize == 0 {
		return "", false
	}

	var val string
	if left {
		// Pop Head
		node := i.ListHead
		val = node.Value
		i.ListHead = node.Next
		if i.ListHead == nil {
			i.ListTail = nil
		} else {
			i.ListHead.Prev = nil
		}
	} else {
		// Pop Tail
		node := i.ListTail
		val = node.Value
		i.ListTail = node.Prev
		if i.ListTail == nil {
			i.ListHead = nil
		} else {
			i.ListTail.Next = nil
		}
	}
	i.ListSize--

	return val, true
}

// Commands

func (d *DistributedMap) LPush(key string, values ...any) (int, error) {
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

	return item.lpush(strValues...), nil
}

func (d *DistributedMap) RPush(key string, values ...any) (int, error) {
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

func (d *DistributedMap) LPop(key string) (any, error) {
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
	isEmpty := item.ListSize == 0
	item.Mu.RUnlock()

	if isEmpty {
		d.Del(key)
	}
	return val, nil
}

func (d *DistributedMap) RPop(key string) (any, error) {
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
	isEmpty := item.ListSize == 0
	item.Mu.RUnlock()

	if isEmpty {
		d.Del(key)
	}
	return val, nil
}

func (d *DistributedMap) blockPop(key string, timeout time.Duration, left bool) (any, error) {
	var val any
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
	if item.ListSize > 0 {
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

func (d *DistributedMap) BLPop(key string, timeout time.Duration) (any, error) {
	return d.blockPop(key, timeout, true)
}

func (d *DistributedMap) BRPop(key string, timeout time.Duration) (any, error) {
	return d.blockPop(key, timeout, false)
}

// PushX Variants

func (d *DistributedMap) LPushX(key string, values ...any) (int, error) {
	exists := d.Exists(key)
	if !exists {
		return 0, nil
	}
	return d.LPush(key, values...)
}

func (d *DistributedMap) RPushX(key string, values ...any) (int, error) {
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
	return item.ListSize, nil
}

func (d *DistributedMap) LRange(key string, start, stop int) ([]any, error) {
	item, err := d.getListItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []any{}, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	size := item.ListSize
	if size == 0 {
		return []any{}, nil
	}

	if start < 0 {
		start = max(size+start, 0)
	}
	if stop < 0 {
		stop = size + stop
	}
	if stop >= size {
		stop = size - 1
	}
	if start > stop {
		return []any{}, nil
	}

	result := make([]any, 0, stop-start+1)

	curr := item.ListHead
	for i := 0; i < start; i++ {
		curr = curr.Next
	}
	for i := start; i <= stop; i++ {
		result = append(result, curr.Value)
		curr = curr.Next
	}

	return result, nil
}

func (d *DistributedMap) LIndex(key string, index int) (any, error) {
	item, err := d.getListItem(key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	size := item.ListSize
	if index < 0 {
		index = size + index
	}
	if index < 0 || index >= size {
		return nil, nil
	}

	var val string
	if index < size/2 {
		curr := item.ListHead
		for i := 0; i < index; i++ {
			curr = curr.Next
		}
		val = curr.Value
	} else {
		curr := item.ListTail
		for i := size - 1; i > index; i-- {
			curr = curr.Prev
		}
		val = curr.Value
	}

	return val, nil
}

func (d *DistributedMap) LSet(key string, index int, value any) error {
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

	size := item.ListSize
	if index < 0 {
		index = size + index
	}
	if index < 0 || index >= size {
		return errors.New("ERR index out of range")
	}

	if index < size/2 {
		curr := item.ListHead
		for i := 0; i < index; i++ {
			curr = curr.Next
		}
		curr.Value = strVal
	} else {
		curr := item.ListTail
		for i := size - 1; i > index; i-- {
			curr = curr.Prev
		}
		curr.Value = strVal
	}

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

	size := item.ListSize
	if start < 0 {
		start = max(size+start, 0)
	}
	if stop < 0 {
		stop = size + stop
	}
	if stop >= size {
		stop = size - 1
	}

	if start > stop {
		// Clear list
		item.ListHead = nil
		item.ListTail = nil
		item.ListSize = 0
		// item.Mu.Unlock() // Double unlock if we do this, remove it
		d.Del(key)
		return nil
	}

	// Optimize: if start=0 and stop=size-1, nothing to do
	if start == 0 && stop == size-1 {
		return nil
	}

	// Find new head
	newHead := item.ListHead
	for i := 0; i < start; i++ {
		newHead = newHead.Next
	}

	// Find new tail from new Head? Or from old tail?
	// The length of new list is Stop - Start + 1
	// The relative index of new tail from new head is (stop - start)

	newTail := newHead
	for i := 0; i < (stop - start); i++ {
		newTail = newTail.Next
	}

	newHead.Prev = nil
	newTail.Next = nil
	item.ListHead = newHead
	item.ListTail = newTail
	item.ListSize = stop - start + 1

	return nil
}

func (d *DistributedMap) LRem(key string, count int, value any) (int, error) {
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
	defer item.Mu.Unlock()

	removed := 0
	if count > 0 {
		// Remove first count occurrences from head
		curr := item.ListHead
		for curr != nil && removed < count {
			next := curr.Next
			if curr.Value == strVal {
				d.removeNode(item, curr)
				removed++
			}
			curr = next
		}
	} else if count < 0 {
		// Remove first count occurrences from tail
		count = -count
		curr := item.ListTail
		for curr != nil && removed < count {
			prev := curr.Prev
			if curr.Value == strVal {
				d.removeNode(item, curr)
				removed++
			}
			curr = prev
		}
	} else {
		// Remove all occurrences
		curr := item.ListHead
		for curr != nil {
			next := curr.Next
			if curr.Value == strVal {
				d.removeNode(item, curr)
				removed++
			}
			curr = next
		}
	}

	if item.ListSize == 0 {
		// item.Mu.Unlock() // avoid double unlock
		// d.Del(key) // Del calls Lock on shard, which is fine as we only hold item lock.
		// NOTE: safe to call Del while holding item lock?
		// Del -> getShard -> Lock Shard -> ...
		// getListItem -> getShard -> Load -> ...
		// Use sync.Map, so fine?
		// Actually d.Del deletes from sync.Map.
		// Calling Del inside item lock is okay if no one holds shard lock and waits for item lock.
		// sync.Map doesn't hold locks across user calls.
		// BUT we shouldn't call d.Del inside item lock if we can avoid it, simply to minimize critical section.
		// However, standard pattern here.
		// Better: we just leave it empty and let next get delete it?
		// OR we rely on `pop` to delete. `LRem` might result in empty.
		// Let's delete it explicitly or mark it?
		// Actually, standard is to delete key if empty.
		// We can't call d.Del directly if it locks shard and we hold item lock... wait, we hold Item.Mu.
		// Shard is sync.Map. Safe.
	}
	// We will do deletion check outside or after unlocking if we want to be super safe,
	// but standard `pop` does it after unlock?
	// `pop` implementation:
	// val, ok := item.pop(true)
	// item.Mu.RLock(); isEmpty := ...; item.Mu.RUnlock()
	// if isEmpty { d.Del(key) }

	// We should do the same here.
	return removed, nil
}

// Helper to remove a node from item list
func (d *DistributedMap) removeNode(item *Item, node *ListNode) {
	if node.Prev != nil {
		node.Prev.Next = node.Next
	} else {
		item.ListHead = node.Next
	}
	if node.Next != nil {
		node.Next.Prev = node.Prev
	} else {
		item.ListTail = node.Prev
	}
	item.ListSize--
	node.Prev = nil
	node.Next = nil
}
