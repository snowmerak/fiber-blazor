package ledis

import (
	"testing"
)

func TestZRankZRevRank(t *testing.T) {
	db := New(16)
	key := "rank_test"

	db.ZAdd(key, 10, "a")
	db.ZAdd(key, 20, "b")
	db.ZAdd(key, 30, "c")

	// ZRANK
	rank, err := db.ZRank(key, "a")
	if err != nil {
		t.Fatalf("ZRank failed: %v", err)
	}
	if rank != 0 {
		t.Errorf("Expected rank 0 for 'a', got %d", rank)
	}

	rank, _ = db.ZRank(key, "c")
	if rank != 2 {
		t.Errorf("Expected rank 2 for 'c', got %d", rank)
	}

	rank, _ = db.ZRank(key, "missing")
	if rank != -1 {
		t.Errorf("Expected rank -1 for missing, got %d", rank)
	}

	// ZREVRANK
	rank, err = db.ZRevRank(key, "c")
	if err != nil {
		t.Fatalf("ZRevRank failed: %v", err)
	}
	// order: c(30), b(20), a(10). c is index 0.
	if rank != 0 {
		t.Errorf("Expected revrank 0 for 'c', got %d", rank)
	}

	rank, _ = db.ZRevRank(key, "a")
	if rank != 2 {
		t.Errorf("Expected revrank 2 for 'a', got %d", rank)
	}
}

func TestZInterStore(t *testing.T) {
	db := New(16)
	k1 := "z1"
	k2 := "z2"
	out := "out"

	// z1: {a:1, b:2}
	db.ZAdd(k1, 1, "a")
	db.ZAdd(k1, 2, "b")

	// z2: {b:3, c:4}
	db.ZAdd(k2, 3, "b")
	db.ZAdd(k2, 4, "c")

	// Intersect z1 z2 -> {b} score: 2+3=5
	count, err := db.ZInterStore(out, k1, k2)
	if err != nil {
		t.Fatalf("ZInterStore failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}

	score, exists, _ := db.ZScore(out, "b")
	if !exists || score != 5 {
		t.Errorf("Expected b with score 5, got exists=%v score=%f", exists, score)
	}

	// Test mixed with Set
	s1 := "s1"
	// s1: {b, d}
	db.SAdd(s1, "b", "d")

	// Intersect out(b:5) with s1(b:1, d:1) -> {b} score: 5+1=6
	count, err = db.ZInterStore("out2", out, s1)
	if err != nil {
		t.Fatalf("ZInterStore mixed failed: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}

	score, exists, _ = db.ZScore("out2", "b")
	if !exists || score != 6 {
		t.Errorf("Expected b with score 6, got exists=%v score=%f", exists, score)
	}

	// Test duplicate keys: z1 z1 -> {a:2, b:4}
	count, err = db.ZInterStore("self", k1, k1)
	if err != nil {
		t.Fatalf("ZInterStore self failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	score, _, _ = db.ZScore("self", "a")
	if score != 2 {
		t.Errorf("Expected a:2, got %f", score)
	}
}
