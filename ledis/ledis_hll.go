package ledis

import (
	"fmt"

	"github.com/axiomhq/hyperloglog"
)

func (d *DistributedMap) getOrCreateHLLItem(key string) (*Item, error) {
	d.mu.RLock()
	shard := d.getShard(key)
	d.mu.RUnlock()

	val, ok := shard.Load(key)
	if !ok {
		item := itemPool.Get().(*Item)
		item.reset()
		item.Type = TypeHyperLogLog
		item.HLL = hyperloglog.New()
		shard.Store(key, item)
		return item, nil
	}

	item := val.(*Item)
	if item.Type != TypeHyperLogLog {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return item, nil
}

func (d *DistributedMap) PfAdd(key string, elements ...string) (int, error) {
	item, err := d.getOrCreateHLLItem(key)
	if err != nil {
		return 0, err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	if item.Type != TypeHyperLogLog {
		return 0, fmt.Errorf("WRONGTYPE")
	}

	if item.HLL == nil {
		item.HLL = hyperloglog.New()
	}

	changed := 0
	oldEst := item.HLL.Estimate()
	for _, el := range elements {
		item.HLL.Insert([]byte(el))
	}
	if item.HLL.Estimate() > oldEst {
		changed = 1
	}

	// Redis returns 1 if at least one internal register was altered, 0 otherwise.
	return changed, nil
}

func (d *DistributedMap) PfCount(keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for 'pfcount' command")
	}

	if len(keys) == 1 {
		item, err := d.Get(keys[0])
		if err != nil {
			return 0, nil // Key missing = 0
		}
		if item == nil {
			return 0, nil
		}

		item.Mu.RLock()
		defer item.Mu.RUnlock()

		if item.Type != TypeHyperLogLog {
			return 0, fmt.Errorf("WRONGTYPE")
		}

		return int64(item.HLL.Estimate()), nil
	}

	// Merge multiple keys into temporary sketch
	temp := hyperloglog.New()
	for _, key := range keys {
		item, err := d.Get(key)
		if err != nil || item == nil {
			continue
		}

		item.Mu.RLock()
		if item.Type == TypeHyperLogLog {
			if err := temp.Merge(item.HLL); err != nil {
				// Should not fail usually
			}
		}
		item.Mu.RUnlock()
	}

	return int64(temp.Estimate()), nil
}

func (d *DistributedMap) PfMerge(destKey string, sourceKeys ...string) error {
	// Get or Create dest
	destItem, err := d.getOrCreateHLLItem(destKey)
	if err != nil {
		return err
	}

	destItem.Mu.Lock()
	defer destItem.Mu.Unlock()

	for _, srcKey := range sourceKeys {
		srcItem, err := d.Get(srcKey)
		if err != nil || srcItem == nil {
			continue
		}

		srcItem.Mu.RLock()
		if srcItem.Type == TypeHyperLogLog {
			if err := destItem.HLL.Merge(srcItem.HLL); err != nil {
				// Log err?
			}
		}
		srcItem.Mu.RUnlock()
	}

	return nil
}

func (d *DistributedMap) RestoreHLL(key string, data []byte) error {
	item, err := d.getOrCreateHLLItem(key)
	if err != nil {
		return err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	if item.Type != TypeHyperLogLog {
		return fmt.Errorf("WRONGTYPE")
	}

	hll := hyperloglog.New()
	if err := hll.UnmarshalBinary(data); err != nil {
		return err
	}
	item.HLL = hll
	return nil
}
