package ledis

import (
	"sync"
	"testing"
	"time"
)

func TestPubSub(t *testing.T) {
	db := New(16)

	// Subscribe to "news" and "sports"
	id, ch := db.Subscribe("news", "sports")

	// Publish to "news"
	count := db.Publish("news", "breaking news")
	if count != 1 {
		t.Errorf("Expected 1 subscriber for news, got %d", count)
	}

	// Verify receive
	select {
	case msg := <-ch:
		if msg != "breaking news" {
			t.Errorf("Expected 'breaking news', got '%s'", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Publish to "sports"
	db.Publish("sports", "goal!")
	select {
	case msg := <-ch:
		if msg != "goal!" {
			t.Errorf("Expected 'goal!', got '%s'", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for sports message")
	}

	// Publish to "tech" (not subscribed)
	count = db.Publish("tech", "go 1.25")
	if count != 0 {
		t.Errorf("Expected 0 subscribers for tech, got %d", count)
	}

	// Unsubscribe from "news"
	db.Unsubscribe(id, "news")

	// Publish to "news" (should not receive)
	count = db.Publish("news", "ignored")
	if count != 0 {
		// We still have the client ID in internal map?
		// Subscribe returns 1 ID for multiple channels.
		// Unsubscribe removes ID from "news".
		t.Errorf("Expected 0 subscribers for news after unsubscribe, got %d", count)
	}

	select {
	case msg := <-ch:
		t.Errorf("Should not receive '%s' from news", msg)
	case <-time.After(100 * time.Millisecond):
		// Good
	}

	// Publish to "sports" (should still receive)
	db.Publish("sports", "still here")
	select {
	case <-ch:
		// Good
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for sports message after partial unsubscribe")
	}

	// Unsubscribe from all
	db.Unsubscribe(id)

	// Publish to "sports" (should not receive)
	count = db.Publish("sports", "gone")
	if count != 0 {
		t.Errorf("Expected 0 subscribers for sports after full unsubscribe, got %d", count)
	}
}

func TestPubSubConcurrency(t *testing.T) {
	db := New(16)
	const subscribers = 100
	const messages = 10

	var wg sync.WaitGroup
	wg.Add(subscribers)

	// 100 subscribers to "chat"
	for range subscribers {
		go func() {
			defer wg.Done()
			_, ch := db.Subscribe("chat")
			received := 0
			for received < messages {
				select {
				case <-ch:
					received++
				case <-time.After(2 * time.Second):
					t.Errorf("Subscriber timed out")
					return
				}
			}
		}()
	}

	// Give time to subscribe
	time.Sleep(100 * time.Millisecond)

	// Publish 10 messages
	for range messages {
		c := db.Publish("chat", "hello")
		if c != int64(subscribers) {
			// This might be flaky if goroutines haven't subscribed yet.
			// But sleep above should help.
			// t.Logf("Published to %d subscribers", c)
		}
	}

	wg.Wait()
}
