package ledis

import (
	"fmt"
	"math"
	"testing"
)

func TestGeo(t *testing.T) {
	db := New(4) // Use small size
	defer db.Close()

	// Add Points
	// Palermo: 38.115556, 13.361389
	// Catania: 37.502669, 15.087269

	n, err := db.GeoAdd("sicily", 38.115556, 13.361389, "Palermo")
	if err != nil || n != 1 {
		t.Errorf("GeoAdd Palermo failed: %v, %d", err, n)
	}

	n, err = db.GeoAdd("sicily", 37.502669, 15.087269, "Catania")
	if err != nil || n != 1 {
		t.Errorf("GeoAdd Catania failed: %v, %d", err, n)
	}

	// Dist matches ~166km
	dist, err := db.GeoDist("sicily", "Palermo", "Catania", "km")
	if err != nil {
		t.Errorf("GeoDist failed: %v", err)
	}
	if math.Abs(dist-166.27) > 1.0 {
		t.Errorf("GeoDist expected ~166.27km, got %f", dist)
	}

	// Radius
	// Search around middle? Or from Palermo with radius 200km (should find Catania)
	// Palermo to Catania is 166km.
	locs, err := db.GeoRadius("sicily", 38.115556, 13.361389, 200, "km")
	if err != nil {
		t.Errorf("GeoRadius failed: %v", err)
	}
	if len(locs) != 2 {
		// Should find Palermo (dist 0) and Catania (dist 166)
		t.Errorf("GeoRadius expected 2 locs, got %d", len(locs))
		for _, l := range locs {
			fmt.Printf("Found: %s dist=%f\n", l.Name, l.Dist)
		}
	} else {
		// Verify
		pFound := false
		cFound := false
		for _, l := range locs {
			if l.Name == "Palermo" {
				pFound = true
			}
			if l.Name == "Catania" {
				cFound = true
			}
		}
		if !pFound || !cFound {
			t.Errorf("GeoRadius missing expected members")
		}
	}
}
