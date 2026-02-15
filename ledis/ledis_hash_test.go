package ledis

import (
	"reflect"
	"sort"
	"testing"
)

func TestHashOperations(t *testing.T) {
	db := New(16)
	key := "myhash"

	// Test HSet
	// HSET myhash field1 "Hello"
	count, err := db.HSet(key, "field1", "Hello")
	if err != nil {
		t.Fatalf("HSet failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 (new field), got %d", count)
	}

	// HSET myhash field1 "World"
	count, err = db.HSet(key, "field1", "World")
	if err != nil {
		t.Fatalf("HSet update failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 (update), got %d", count)
	}

	// Test HGet
	val, err := db.HGet(key, "field1")
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if val != "World" {
		t.Errorf("Expected 'World', got %v", val)
	}

	val, err = db.HGet(key, "missing")
	if val != nil {
		t.Errorf("Expected nil for missing field, got %v", val)
	}

	// Test HExists
	exists, _ := db.HExists(key, "field1")
	if !exists {
		t.Errorf("Expected field1 to exist")
	}

	// Test HMSet
	err = db.HMSet(key, map[string]any{
		"field2": "Value2",
		"field3": "Value3",
	})
	if err != nil {
		t.Fatalf("HMSet failed: %v", err)
	}

	// Test HMGet
	vals, err := db.HMGet(key, "field1", "field2", "missing")
	if err != nil {
		t.Fatalf("HMGet failed: %v", err)
	}
	expected := []any{"World", "Value2", nil}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("HMGet wrong. Expected %v, got %v", expected, vals)
	}

	// Test HLen
	l, _ := db.HLen(key)
	if l != 3 {
		t.Errorf("Expected HLen 3, got %d", l)
	}

	// Test HKeys
	keys, _ := db.HKeys(key)
	sort.Strings(keys)
	expectedKeys := []string{"field1", "field2", "field3"}
	if !reflect.DeepEqual(keys, expectedKeys) {
		t.Errorf("HKeys wrong. Expected %v, got %v", expectedKeys, keys)
	}

	// Test HGetAll
	all, _ := db.HGetAll(key)
	if len(all) != 3 {
		t.Errorf("HGetAll returned wrong size")
	}
	if all["field1"] != "World" {
		t.Errorf("HGetAll content mismatch")
	}

	// Test HDel
	delCount, _ := db.HDel(key, "field1", "field3", "missing")
	if delCount != 2 {
		t.Errorf("Expected 2 deleted, got %d", delCount)
	}

	// Check cleanup
	l, _ = db.HLen(key)
	if l != 1 {
		t.Errorf("Expected HLen 1, got %d", l)
	}

	db.HDel(key, "field2")
	exists = db.Exists(key) // Key should be deleted if empty?
	// Implementation: HDel deletes key if empty.
	if exists {
		t.Errorf("Hash key should be deleted when empty")
	}
}

func TestHIncrBy(t *testing.T) {
	db := New(16)
	key := "inx_hash"

	val, err := db.HIncrBy(key, "cnt", 10)
	if err != nil {
		t.Fatalf("HIncrBy failed: %v", err)
	}
	if val != 10 {
		t.Errorf("Expected 10, got %d", val)
	}

	val, _ = db.HIncrBy(key, "cnt", -5)
	if val != 5 {
		t.Errorf("Expected 5, got %d", val)
	}
}
