package ledis

import (
	"fmt"
	"testing"
)

func TestXTrim(t *testing.T) {
	db := New(1)
	defer db.Close()

	key := "s_trim"

	// Add 10 items
	for i := 0; i < 10; i++ {
		_, err := db.XAdd(key, "*", 0, "k", fmt.Sprintf("v%d", i))
		if err != nil {
			t.Fatalf("XAdd failed: %v", err)
		}
	}

	// Check len
	l, _ := db.XLen(key)
	if l != 10 {
		t.Fatalf("Expected len 10, got %d", l)
	}

	// Trim to 5
	deleted, err := db.XTrim(key, 5)
	if err != nil {
		t.Fatalf("XTrim failed: %v", err)
	}
	if deleted != 5 {
		t.Errorf("Expected deleted 5, got %d", deleted)
	}

	l, _ = db.XLen(key)
	if l != 5 {
		t.Errorf("Expected len 5 after trim, got %d", l)
	}

	// Verify remaining are the last 5
	entries, _ := db.XRange(key, "-", "+")
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}
	// Check content of first remaining entry (should be v5)
	if entries[0].Fields[1] != "v5" {
		t.Errorf("Expected first entry to be v5, got %s", entries[0].Fields[1])
	}
}

func TestXAddMaxLen(t *testing.T) {
	db := New(1)
	defer db.Close()

	key := "s_maxlen"

	// Add 5 items with MaxLen 3
	for i := 0; i < 5; i++ {
		_, err := db.XAdd(key, "*", 3, "k", fmt.Sprintf("v%d", i))
		if err != nil {
			t.Fatalf("XAdd failed: %v", err)
		}
	}

	// Check len should be 3
	l, _ := db.XLen(key)
	if l != 3 {
		t.Errorf("Expected len 3, got %d", l)
	}

	// Verify remaining are v2, v3, v4
	entries, _ := db.XRange(key, "-", "+")
	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries")
	}
	if entries[0].Fields[1] != "v2" {
		t.Errorf("Expected first entry v2, got %s", entries[0].Fields[1])
	}
	if entries[2].Fields[1] != "v4" {
		t.Errorf("Expected last entry v4, got %s", entries[2].Fields[1])
	}
}
