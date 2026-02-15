package ledis

import (
	"testing"
)

func TestStringOperations(t *testing.T) {
	db := New(16)

	// Test GetSet
	db.Set("key1", "val1", 0)
	oldVal, ok := db.GetSet("key1", "val2")
	if !ok || oldVal != "val1" {
		t.Errorf("GetSet failed, expected val1, got %v", oldVal)
	}
	newVal, _ := db.Get("key1")
	if newVal != "val2" {
		t.Errorf("GetSet failed to set new value, expected val2, got %v", newVal)
	}

	// Test MSet and MGet
	db.MSet(map[string]any{
		"mkey1": "mval1",
		"mkey2": "mval2",
	})
	vals := db.MGet("mkey1", "mkey2", "missing")
	if len(vals) != 3 {
		t.Errorf("MGet returned wrong number of values")
	}
	if vals[0] != "mval1" || vals[1] != "mval2" || vals[2] != nil {
		t.Errorf("MGet returned incorrect values: %v", vals)
	}
}

func TestIncrDecr(t *testing.T) {
	db := New(16)

	// Test Incr on new key
	val, err := db.Incr("counter")
	if err != nil {
		t.Errorf("Incr failed: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	// Test Incr on existing key
	val, _ = db.Incr("counter")
	if val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}

	// Test Decr
	val, err = db.Decr("counter")
	if err != nil {
		t.Errorf("Decr failed: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	// Test IncrBy
	val, _ = db.IncrBy("counter", 10)
	if val != 11 {
		t.Errorf("Expected 11, got %d", val)
	}

	// Test DecrBy
	val, _ = db.DecrBy("counter", 5)
	if val != 6 {
		t.Errorf("Expected 6, got %d", val)
	}
}

func TestAppendStrLen(t *testing.T) {
	db := New(16)

	// Test Append
	db.Set("strKey", "Hello", 0)
	lenVal, err := db.Append("strKey", " World")
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}
	if lenVal != 11 {
		t.Errorf("Expected length 11, got %d", lenVal)
	}

	val, _ := db.Get("strKey")
	if val != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%v'", val)
	}

	// Test StrLen
	l, err := db.StrLen("strKey")
	if err != nil {
		t.Errorf("StrLen failed: %v", err)
	}
	if l != 11 {
		t.Errorf("Expected 11, got %d", l)
	}
}

func TestParallelIncr(t *testing.T) {
	db := New(16)
	key := "concurrent_counter"
	count := 1000

	done := make(chan bool)

	for range count {
		go func() {
			_, err := db.Incr(key)
			if err != nil {
				t.Error(err)
			}
			done <- true
		}()
	}

	for range count {
		<-done
	}

	val, ok := db.Get(key)
	if !ok {
		t.Errorf("Counter key missing")
	}

	// Since we are storing int64 in Incr, we need to cast correctly or toInt64 helper
	intVal, err := toInt64(val)
	if err != nil {
		t.Errorf("Failed to cast value: %v", val)
	}

	if intVal != int64(count) {
		t.Errorf("Expected %d, got %d", count, intVal)
	}
}

// Helper need to replicate here or make public?
// toInt64 is private in ledis global scope (same package), so tests in same package can use it?
// Yes, tests in `package ledis` can access private members of `package ledis`.
// Wait, toInt64 is defined in ledis_string.go, which is part of package ledis.
// So it is accessible.
