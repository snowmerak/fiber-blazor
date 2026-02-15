package ledis

import (
	"reflect"
	"testing"
	"time"
)

func TestPushX(t *testing.T) {
	db := New(16)
	key := "pushx_list"

	// List doesn't exist, LPushX should fail (return 0)
	count, err := db.LPushX(key, "a")
	if err != nil {
		t.Fatalf("LPushX failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0, got %d", count)
	}

	// Create list
	db.LPush(key, "init")

	// LPushX should work now
	count, err = db.LPushX(key, "b")
	if err != nil {
		t.Fatalf("LPushX failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2, got %d", count)
	}

	vals, _ := db.LRange(key, 0, -1)
	expected := []string{"b", "init"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("LPushX values wrong. Found %v", vals)
	}
}

func TestBlockingPop(t *testing.T) {
	db := New(16)
	key := "blpop_list"

	// 1. BLPop on empty list with timeout
	start := time.Now()
	val, err := db.BLPop(key, 200*time.Millisecond)
	elapsed := time.Since(start)

	if err != ErrTimeout {
		t.Errorf("Expected ErrTimeout, got %v", err)
	}
	if val != "" {
		t.Errorf("Expected empty string value, got %v", val)
	}
	if elapsed < 200*time.Millisecond {
		t.Errorf("Blocking didn't wait long enough: %v", elapsed)
	}

	// 2. BLPop successfully receiving data
	go func() {
		time.Sleep(100 * time.Millisecond)
		db.LPush(key, "data")
	}()

	start = time.Now()
	// Long timeout, should return early
	val, err = db.BLPop(key, 1*time.Second)
	if err != nil {
		t.Errorf("BLPop failed: %v", err)
	}
	if val != "data" {
		t.Errorf("Expected 'data', got %v", val)
	}

	// 3. Multiple waiters logic test
	// If wait times are slightly staggered, we still test notification works.
	// Since `LPush` serves waiters in FCFS (based on slice queue), we need to ensure they are queued correctly.
	// We'll trust basic `LPush` logic for now as race conditions in test setup are tricky.
}

func TestMixedPop(t *testing.T) {
	db := New(16)
	key := "mixed_pop"

	db.LPush(key, "existing")

	// BLPop should return immediately
	start := time.Now()
	val, err := db.BLPop(key, 1*time.Second)
	if err != nil {
		t.Errorf("BLPop immediate failed: %v", err)
	}
	if val != "existing" {
		t.Errorf("Expected 'existing', got %v", val)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Errorf("BLPop took too long for existing item: %v", time.Since(start))
	}
}
