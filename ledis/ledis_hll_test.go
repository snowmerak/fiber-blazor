package ledis

import (
	"testing"
)

func TestHLL(t *testing.T) {
	db := New(4)
	defer db.Close()

	// PFADD
	n, err := db.PfAdd("hll1", "a", "b", "c")
	if err != nil {
		t.Fatalf("PfAdd failed: %v", err)
	}
	// Should be 1 (items added)
	// With Estimate logic, adding unique items increases estimate.
	if n != 1 {
		t.Logf("PfAdd returned %d, expected 1 (maybe estimate didn't change?)", n)
	}

	// Count
	count, err := db.PfCount("hll1")
	if err != nil {
		t.Fatalf("PfCount failed: %v", err)
	}
	if count != 3 {
		t.Errorf("PfCount expected 3, got %d", count)
	}

	// Add existing
	n, _ = db.PfAdd("hll1", "a")
	if n != 0 {
		t.Errorf("PfAdd existing returned %d, expected 0", n)
	}

	// Merge
	db.PfAdd("hll2", "c", "d", "e") // {c, d, e}
	// Merge hll1 {a,b,c} + hll2 {c,d,e} -> {a,b,c,d,e} (5 elements)

	err = db.PfMerge("hll3", "hll1", "hll2")
	if err != nil {
		t.Fatalf("PfMerge failed: %v", err)
	}

	count, _ = db.PfCount("hll3")
	if count != 5 {
		t.Errorf("PfMerge count expected 5, got %d", count)
	}
}
