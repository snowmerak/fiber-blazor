package ledis

// Subscribe subscribes the client to the specified channels.
// Returns a channel that receives messages and a clientID.
// The caller should read from this channel.
// Note: Real Redis SUBSCRIBE blocks the connection.
// Here we return a Go channel for the caller to consume.
func (d *DistributedMap) Subscribe(channels ...string) (int64, chan string) {
	d.pubsub.mu.Lock()
	defer d.pubsub.mu.Unlock()

	id := d.pubsub.nextID
	d.pubsub.nextID++

	msgChan := make(chan string, 1024) // Buffer to avoid blocking publisher too easily

	for _, ch := range channels {
		if _, ok := d.pubsub.channels[ch]; !ok {
			d.pubsub.channels[ch] = make(map[int64]chan string)
		}
		d.pubsub.channels[ch][id] = msgChan
	}

	return id, msgChan
}

// Unsubscribe unsubscribes the client from the specified channels.
// If no channels are provided, unsubscribes from all.
func (d *DistributedMap) Unsubscribe(id int64, channels ...string) {
	d.pubsub.mu.Lock()
	defer d.pubsub.mu.Unlock()

	if len(channels) == 0 {
		// Unsubscribe from all
		for ch, clients := range d.pubsub.channels {
			if _, ok := clients[id]; ok {
				delete(clients, id)
				if len(clients) == 0 {
					delete(d.pubsub.channels, ch)
				}
			}
		}
		return
	}

	for _, ch := range channels {
		if clients, ok := d.pubsub.channels[ch]; ok {
			delete(clients, id)
			if len(clients) == 0 {
				delete(d.pubsub.channels, ch)
			}
		}
	}
}

// Publish posts a message to the given channel.
func (d *DistributedMap) Publish(channel string, message string) int64 {
	d.pubsub.mu.RLock()
	defer d.pubsub.mu.RUnlock()

	clients, ok := d.pubsub.channels[channel]
	if !ok {
		return 0
	}

	count := int64(0)
	for _, ch := range clients {
		// Non-blocking send to avoid stalling publisher if subscriber is slow
		select {
		case ch <- message:
			count++
		default:
			// Drop message if buffer full? Or wait?
			// Redis drops if buffer limit reached, but here we just drop for simplicity/safety.
		}
	}
	return count
}

// PubSubChannels returns active channels (matching pattern is optional but here we list all)
func (d *DistributedMap) PubSubChannels(pattern string) []string {
	d.pubsub.mu.RLock()
	defer d.pubsub.mu.RUnlock()

	var channels []string
	for ch := range d.pubsub.channels {
		// Simple match or all
		if pattern == "" || pattern == "*" { // Rudimentary pattern support
			channels = append(channels, ch)
		} else if ch == pattern {
			channels = append(channels, ch)
		}
	}
	return channels
}

// PubSubNumSub returns the number of subscribers for the specified channels.
func (d *DistributedMap) PubSubNumSub(channels ...string) map[string]int64 {
	d.pubsub.mu.RLock()
	defer d.pubsub.mu.RUnlock()

	result := make(map[string]int64)
	for _, ch := range channels {
		if clients, ok := d.pubsub.channels[ch]; ok {
			result[ch] = int64(len(clients))
		} else {
			result[ch] = 0
		}
	}
	return result
}
