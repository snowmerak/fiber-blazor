package ledis

import (
	"testing"
)

func TestStringOperations(t *testing.T) {
	db := New(16)

	// Test GetSet
	db.Set("key1", "val1", 0)
	oldVal, ok, _ := db.GetSet("key1", "val2")
	if !ok || oldVal != "val1" {
		t.Errorf("GetSet failed, expected val1, got %v", oldVal)
	}
	newItem, _ := db.Get("key1")
	if newItem.Str != "val2" {
		t.Errorf("GetSet failed to set new value, expected val2, got %v", newItem.Str)
	}

	// Test MSet and MGet
	db.MSet(map[string]any{
		"mkey1": "mval1",
		"mkey2": "mval2",
	})
	items := db.MGet("mkey1", "mkey2", "missing")
	if len(items) != 3 {
		t.Errorf("MGet returned wrong number of values")
	}
	if items[0].Str != "mval1" || items[1].Str != "mval2" || items[2] != nil {
		t.Errorf("MGet returned incorrect values")
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
	if val.Str != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%v'", val.Str)
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

	val, err := db.Get(key)
	if err != nil {
		t.Errorf("Counter key missing")
	}

	// Since we are storing int64 in Incr, we need to cast correctly or toInt64 helper
	intVal, err := toInt64(val.Str)
	if err != nil {
		t.Errorf("Failed to cast value: %v", val.Str)
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
