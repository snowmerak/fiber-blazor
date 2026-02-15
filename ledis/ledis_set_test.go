package ledis

import (
	"reflect"
	"sort"
	"testing"
)

func TestSetBasics(t *testing.T) {
	db := New(16)
	key := "myset"

	// Test SAdd
	// SADD myset a b c
	count, err := db.SAdd(key, "a", "b", "c")
	if err != nil {
		t.Fatalf("SAdd failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 added, got %d", count)
	}

	// SADD myset a (duplicate)
	count, err = db.SAdd(key, "a")
	if err != nil {
		t.Fatalf("SAdd duplicate failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 added for duplicate, got %d", count)
	}

	// Test SIsMember
	exists, err := db.SIsMember(key, "a")
	if err != nil {
		t.Fatalf("SIsMember failed: %v", err)
	}
	if !exists {
		t.Errorf("Expected 'a' to be member")
	}

	exists, _ = db.SIsMember(key, "missing")
	if exists {
		t.Errorf("Expected 'missing' not to be member")
	}

	// Test SCard
	card, err := db.SCard(key)
	if err != nil {
		t.Errorf("SCard failed: %v", err)
	}
	if card != 3 {
		t.Errorf("Expected cardinality 3, got %d", card)
	}

	// Test SMembers
	members, err := db.SMembers(key)
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}
	// Sort to compare
	strMembers := make([]string, len(members))
	for i, m := range members {
		strMembers[i] = m.(string)
	}
	sort.Strings(strMembers)
	expected := []string{"a", "b", "c"}
	if !reflect.DeepEqual(strMembers, expected) {
		t.Errorf("SMembers wrong. Expected %v, got %v", expected, strMembers)
	}

	// Test SRem
	removed, err := db.SRem(key, "a", "missing")
	if err != nil {
		t.Fatalf("SRem failed: %v", err)
	}
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}

	exists, _ = db.SIsMember(key, "a")
	if exists {
		t.Errorf("Expected 'a' to be removed")
	}
}

func TestSetOperations(t *testing.T) {
	db := New(16)
	k1 := "set1"
	k2 := "set2"

	// set1: {a, b, c}
	db.SAdd(k1, "a", "b", "c")
	// set2: {b, c, d}
	db.SAdd(k2, "b", "c", "d")

	// Test SInter
	inter, err := db.SInter(k1, k2)
	if err != nil {
		t.Fatalf("SInter failed: %v", err)
	}
	strInter := toStringSlice(inter)
	sort.Strings(strInter)
	expectedInter := []string{"b", "c"}
	if !reflect.DeepEqual(strInter, expectedInter) {
		t.Errorf("SInter wrong. Expected %v, got %v", expectedInter, strInter)
	}

	// Test SUnion
	union, err := db.SUnion(k1, k2)
	if err != nil {
		t.Fatalf("SUnion failed: %v", err)
	}
	strUnion := toStringSlice(union)
	sort.Strings(strUnion)
	expectedUnion := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(strUnion, expectedUnion) {
		t.Errorf("SUnion wrong. Expected %v, got %v", expectedUnion, strUnion)
	}

	// Test SDiff (set1 - set2) -> {a}
	diff, err := db.SDiff(k1, k2)
	if err != nil {
		t.Fatalf("SDiff failed: %v", err)
	}
	strDiff := toStringSlice(diff)
	// Should be just "a"
	if len(strDiff) != 1 || strDiff[0] != "a" {
		t.Errorf("SDiff wrong. Expected [a], got %v", strDiff)
	}
}

func TestSMove(t *testing.T) {
	db := New(16)
	src := "src"
	dst := "dst"

	db.SAdd(src, "member")
	db.SAdd(dst, "other")

	// Move "member" from src to dst
	moved, err := db.SMove(src, dst, "member")
	if err != nil {
		t.Fatalf("SMove failed: %v", err)
	}
	if !moved {
		t.Errorf("Expected SMove to return true")
	}

	// Verify src empty (and deleted?)
	exists, _ := db.SIsMember(src, "member")
	if exists {
		t.Errorf("Member still in source")
	}

	// Verify dst has member
	exists, _ = db.SIsMember(dst, "member")
	if !exists {
		t.Errorf("Member not in destination")
	}
}

func TestSPop(t *testing.T) {
	db := New(16)
	key := "spop_set"
	db.SAdd(key, "one", "two", "three")

	val, err := db.SPop(key)
	if err != nil {
		t.Fatalf("SPop failed: %v", err)
	}
	if val == nil {
		t.Errorf("Expected value, got nil")
	}

	// Should be removed
	exists, _ := db.SIsMember(key, val)
	if exists {
		t.Errorf("SPop did not remove element")
	}

	card, _ := db.SCard(key)
	if card != 2 {
		t.Errorf("Expected card 2, got %d", card)
	}
}

func toStringSlice(interfaces []any) []string {
	strs := make([]string, len(interfaces))
	for i, v := range interfaces {
		strs[i] = v.(string)
	}
	return strs
}
