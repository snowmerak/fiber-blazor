package ledis

import (
	"reflect"
	"strconv"
	"testing"
)

func TestZSetBasics(t *testing.T) {
	db := New(16)
	key := "myzset"

	// ZADD myzset 1 "one"
	added, err := db.ZAdd(key, 1, "one")
	if err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	if added != 1 {
		t.Errorf("Expected 1 added, got %d", added)
	}

	// ZADD myzset 1 "uno"
	added, err = db.ZAdd(key, 1, "uno")
	if err != nil {
		t.Fatalf("ZAdd failed: %v", err)
	}
	if added != 1 {
		t.Errorf("Expected 1 added, got %d", added)
	}

	// ZADD myzset 2 "two" 3 "three"
	// My client supports one pair per call in interface?
	// func (d *DistributedMap) ZAdd(key string, score float64, member string) (int, error)
	// It only supports single add.
	db.ZAdd(key, 2, "two")
	db.ZAdd(key, 3, "three")

	// ZSCORE
	score, exists, err := db.ZScore(key, "one")
	if err != nil {
		t.Fatalf("ZScore failed: %v", err)
	}
	if !exists {
		t.Errorf("Expected 'one' to exist")
	}
	if score != 1 {
		t.Errorf("Expected score 1, got %f", score)
	}

	// ZCARD
	card, err := db.ZCard(key)
	if card != 4 {
		t.Errorf("Expected card 4, got %d", card)
	}

	// ZINCRBY
	newScore, err := db.ZIncrBy(key, 2, "one")
	if err != nil {
		t.Fatalf("ZIncrBy failed: %v", err)
	}
	if newScore != 3 {
		t.Errorf("Expected new score 3, got %f", newScore)
	}

	// Check if updated in ZSet (SkipList)
	score, _, _ = db.ZScore(key, "one")
	if score != 3 {
		t.Errorf("ZScore after IncrBy wrong: %f", score)
	}
}

func TestZRange(t *testing.T) {
	db := New(16)
	key := "zrange_test"

	// Insert: a:1, b:2, c:3
	db.ZAdd(key, 1, "a")
	db.ZAdd(key, 2, "b")
	db.ZAdd(key, 3, "c")

	// ZRANGE 0 -1 (All)
	res, err := db.ZRange(key, 0, -1, false)
	if err != nil {
		t.Fatalf("ZRange failed: %v", err)
	}

	expected := []string{"a", "b", "c"}
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("ZRange 0 -1 wrong. Got %v", res)
	}

	// ZRANGE 0 1
	res, err = db.ZRange(key, 0, 1, false)
	expected = []string{"a", "b"}
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("ZRange 0 1 wrong. Got %v", res)
	}

	// ZREVRANGE 0 -1 -> c, b, a
	res, err = db.ZRevRange(key, 0, -1, false)
	if err != nil {
		t.Fatalf("ZRevRange failed: %v", err)
	}
	expected = []string{"c", "b", "a"}
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("ZRevRange 0 -1 wrong. Got %v", res)
	}

	// ZREVRANGE 0 0 -> c
	res, err = db.ZRevRange(key, 0, 0, false)
	expected = []string{"c"}
	if !reflect.DeepEqual(res, expected) {
		t.Errorf("ZRevRange 0 0 wrong. Got %v", res)
	}
}

func TestZRem(t *testing.T) {
	db := New(16)
	key := "zrem_test"

	db.ZAdd(key, 10, "del_me")
	count, err := db.ZRem(key, "del_me")
	if err != nil {
		t.Fatalf("ZRem failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 removed, got %d", count)
	}

	_, exists, _ := db.ZScore(key, "del_me")
	if exists {
		t.Errorf("Expected 'del_me' to be removed")
	}
}

func TestSkipListIntegrity(t *testing.T) {
	// Add many items to trigger multiple levels
	db := New(16)
	key := "stress"

	n := 100
	for i := range n {
		db.ZAdd(key, float64(i), strconv.Itoa(i))
	}

	card, _ := db.ZCard(key)
	if card != int64(n) {
		t.Errorf("Card mismatch: %d vs %d", card, n)
	}

	// Verify range order
	res, _ := db.ZRange(key, 0, -1, true) // with scores
	// res should be: "0", 0, "1", 1...
	if len(res) != n*2 {
		t.Errorf("ZRangeWithScores length wrong: %d", len(res))
	}

	// Check a few
	if res[0] != "0" || res[1] != "0" {
		t.Errorf("First element wrong: %v, %v", res[0], res[1])
	}
}
