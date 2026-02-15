package ledis

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBasicOperations(t *testing.T) {
	db := New(16)

	// Test Set and Get
	db.Set("key1", "value1", 0)
	val, err := db.Get("key1")
	if err != nil {
		t.Errorf("Expected key1 to exist, got error: %v", err)
	}
	if val.Str != "value1" {
		t.Errorf("Expected value1, got %v", val.Str)
	}

	// Test Exists
	if !db.Exists("key1") {
		t.Errorf("Expected key1 to exist")
	}

	// Test Del
	db.Del("key1")
	if db.Exists("key1") {
		t.Errorf("Expected key1 to be deleted")
	}
	_, err = db.Get("key1")
	if err == nil {
		t.Errorf("Expected key1 to be not found after deletion")
	}
}

func TestTTL(t *testing.T) {
	db := New(16)

	// Test Expiry
	db.Set("key_ttl", "value_ttl", 100*time.Millisecond)
	if !db.Exists("key_ttl") {
		t.Errorf("Expected key_ttl to exist initially")
	}

	time.Sleep(200 * time.Millisecond)

	if db.Exists("key_ttl") {
		t.Errorf("Expected key_ttl to expire")
	}
	_, err := db.Get("key_ttl")
	if err == nil {
		t.Errorf("Expected key_ttl to be not found after expiry")
	}
}

func TestConcurrency(t *testing.T) {
	db := New(1024) // Larger size to reduce collision probability in sharding visualization if we were tracing

	var wg sync.WaitGroup
	numRoutines := 100
	numOps := 1000

	// Concurrent Sets
	wg.Add(numRoutines)
	for i := range numRoutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOps {
				key := fmt.Sprintf("key-%d-%d", id, j)
				db.Set(key, j, 0)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent Gets
	wg.Add(numRoutines)
	for i := range numRoutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOps {
				key := fmt.Sprintf("key-%d-%d", id, j)
				val, err := db.Get(key)
				if err != nil {
					t.Errorf("Expected %s to exist", key)
				}
				if val.Str != fmt.Sprintf("%d", j) {
					t.Errorf("Expected %d, got %v", j, val.Str)
				}
			}
		}(i)
	}
	wg.Wait()
}
