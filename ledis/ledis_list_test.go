package ledis

import (
	"reflect"
	"testing"
)

func TestListBasics(t *testing.T) {
	db := New(16)
	key := "mylist"

	// Test LPush
	// LPUSH mylist a b c -> [c, b, a]
	count, err := db.LPush(key, "a", "b", "c")
	if err != nil {
		t.Fatalf("LPush failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected length 3, got %d", count)
	}

	// Test LRange all
	vals, err := db.LRange(key, 0, -1)
	if err != nil {
		t.Fatalf("LRange failed: %v", err)
	}
	expected := []interface{}{"c", "b", "a"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("LPush order wrong. Expected %v, got %v", expected, vals)
	}

	// Test RPush
	// RPUSH mylist 1 2 -> [c, b, a, 1, 2]
	count, err = db.RPush(key, 1, 2)
	if err != nil {
		t.Fatalf("RPush failed: %v", err)
	}
	if count != 5 {
		t.Errorf("Expected length 5, got %d", count)
	}

	vals, _ = db.LRange(key, 0, -1)
	expected = []interface{}{"c", "b", "a", "1", "2"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("RPush order wrong. Expected %v, got %v", expected, vals)
	}

	// Test LLen
	lenVal, err := db.LLen(key)
	if err != nil {
		t.Errorf("LLen failed: %v", err)
	}
	if lenVal != 5 {
		t.Errorf("Expected LLen 5, got %d", lenVal)
	}

	// Test LPop
	val, err := db.LPop(key)
	if err != nil {
		t.Fatalf("LPop failed: %v", err)
	}
	if val != "c" {
		t.Errorf("Expected 'c', got %v", val)
	}

	// Test RPop
	val, err = db.RPop(key)
	if err != nil {
		t.Fatalf("RPop failed: %v", err)
	}
	if val != "2" {
		t.Errorf("Expected '2', got %v", val)
	}

	// Remaining: [b, a, 1]
	vals, _ = db.LRange(key, 0, -1)
	expected = []interface{}{"b", "a", "1"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("List state wrong after pops. Expected %v, got %v", expected, vals)
	}
}

func TestListManipulation(t *testing.T) {
	db := New(16)
	key := "list_manip"

	// Setup: [one, two, three, four, five]
	db.RPush(key, "one", "two", "three", "four", "five")

	// Test LIndex
	val, err := db.LIndex(key, 2)
	if err != nil {
		t.Errorf("LIndex failed: %v", err)
	}
	if val != "three" {
		t.Errorf("Expected 'three', got %v", val)
	}

	val, _ = db.LIndex(key, -1)
	if val != "five" {
		t.Errorf("Expected 'five' for index -1, got %v", val)
	}

	val, _ = db.LIndex(key, 100)
	if val != nil {
		t.Errorf("Expected nil for out of range index, got %v", val)
	}

	// Test LSet
	err = db.LSet(key, 1, "2")
	if err != nil {
		t.Errorf("LSet failed: %v", err)
	}
	val, _ = db.LIndex(key, 1)
	if val != "2" {
		t.Errorf("Expected '2', got %v", val)
	}

	err = db.LSet(key, 100, "x")
	if err == nil {
		t.Errorf("Expected error for LSet out of range")
	}

	// Test LTrim
	// List: [one, 2, three, four, five]
	// LTrim 1 -2 (indices 1 to second to last) -> [2, three, four]
	err = db.LTrim(key, 1, -2)
	if err != nil {
		t.Errorf("LTrim failed: %v", err)
	}
	vals, _ := db.LRange(key, 0, -1)
	expected := []interface{}{"2", "three", "four"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("LTrim result wrong. Expected %v, got %v", expected, vals)
	}
}

func TestLRem(t *testing.T) {
	db := New(16)
	key := "rem_list"

	// [a, b, a, c, a, d]
	db.RPush(key, "a", "b", "a", "c", "a", "d")

	// Remove 2 'a' from head
	count, err := db.LRem(key, 2, "a")
	if err != nil {
		t.Errorf("LRem failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 removed, got %d", count)
	}

	// Expected: [b, c, a, d]
	vals, _ := db.LRange(key, 0, -1)
	expected := []interface{}{"b", "c", "a", "d"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("LRem(2, a) result wrong. Expected %v, got %v", expected, vals)
	}

	db.Del(key)
	// [a, b, a, c, a, d]
	db.RPush(key, "a", "b", "a", "c", "a", "d")

	// Remove 2 'a' from tail
	db.LRem(key, -2, "a")
	// Expected: [a, b, c, d]
	vals, _ = db.LRange(key, 0, -1)
	expected = []interface{}{"a", "b", "c", "d"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("LRem(-2, a) result wrong. Expected %v, got %v", expected, vals)
	}

	db.Del(key)
	db.RPush(key, "a", "b", "a")
	// Remove all 'a'
	db.LRem(key, 0, "a")
	vals, _ = db.LRange(key, 0, -1)
	expected = []interface{}{"b"}
	if !reflect.DeepEqual(vals, expected) {
		t.Errorf("LRem(0, a) result wrong. Expected %v, got %v", expected, vals)
	}
}

func TestListTypeMismatch(t *testing.T) {
	db := New(16)
	db.Set("str_key", "value", 0)

	_, err := db.LPush("str_key", "a")
	if err != ErrWrongType {
		t.Errorf("Expected ErrWrongType, got %v", err)
	}
}
