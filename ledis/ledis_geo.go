package ledis

import (
	"fmt"
	"math"
	"sort"
)

// Grid size ~ 1km
const (
	gridLatStep = 0.01
	gridLonStep = 0.01
)

func getGridKey(lat, lon float64) string {
	latIdx := int(math.Floor(lat / gridLatStep))
	lonIdx := int(math.Floor(lon / gridLatStep))
	return fmt.Sprintf("%d:%d", latIdx, lonIdx)
}

func (d *DistributedMap) getOrCreateGeoItem(key string) (*Item, error) {
	d.mu.RLock()
	shard := d.getShard(key)
	d.mu.RUnlock()

	val, ok := shard.Load(key)
	if !ok {
		item := itemPool.Get().(*Item)
		item.reset()
		item.Type = TypeGeo
		item.Geo = &Geo{
			members: make(map[string]GeoPoint),
			grid:    make(map[string]map[string]struct{}),
		}
		shard.Store(key, item)
		return item, nil
	}

	item := val.(*Item)
	if item.Type != TypeGeo {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return item, nil
}

func (d *DistributedMap) GeoAdd(key string, lat, lon float64, member string) (int, error) {
	item, err := d.getOrCreateGeoItem(key)
	if err != nil {
		return 0, err
	}

	item.Mu.Lock()
	defer item.Mu.Unlock()

	if item.Type != TypeGeo {
		return 0, fmt.Errorf("WRONGTYPE")
	}

	if item.Geo == nil {
		item.Geo = &Geo{
			members: make(map[string]GeoPoint),
			grid:    make(map[string]map[string]struct{}),
		}
	}

	gridKey := getGridKey(lat, lon)

	count := 0
	oldPoint, exists := item.Geo.members[member]
	if exists {
		oldGridKey := getGridKey(oldPoint.Lat, oldPoint.Lon)
		if oldGridKey != gridKey {
			// Remove from old grid
			if members, ok := item.Geo.grid[oldGridKey]; ok {
				delete(members, member)
				if len(members) == 0 {
					delete(item.Geo.grid, oldGridKey)
				}
			}
		} else {
			// Same grid, just update
		}
	} else {
		count = 1
	}

	// Update member
	item.Geo.members[member] = GeoPoint{Lat: lat, Lon: lon}

	// Add to new grid
	if _, ok := item.Geo.grid[gridKey]; !ok {
		item.Geo.grid[gridKey] = make(map[string]struct{})
	}
	item.Geo.grid[gridKey][member] = struct{}{}

	return count, nil
}

func haversine(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000 // meters
	phi1 := lat1 * math.Pi / 180
	phi2 := lat2 * math.Pi / 180
	dphi := (lat2 - lat1) * math.Pi / 180
	dlambda := (lon2 - lon1) * math.Pi / 180

	a := math.Sin(dphi/2)*math.Sin(dphi/2) +
		math.Cos(phi1)*math.Cos(phi2)*
			math.Sin(dlambda/2)*math.Sin(dlambda/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return R * c
}

func (d *DistributedMap) GeoDist(key, m1, m2, unit string) (float64, error) {
	item, err := d.Get(key)
	if err != nil {
		return 0, err
	}
	if item == nil {
		return 0, fmt.Errorf("key not found")
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	if item.Type != TypeGeo {
		return 0, fmt.Errorf("WRONGTYPE")
	}

	p1, ok1 := item.Geo.members[m1]
	p2, ok2 := item.Geo.members[m2]

	if !ok1 || !ok2 {
		return 0, fmt.Errorf("member not found")
	}

	distMeters := haversine(p1.Lat, p1.Lon, p2.Lat, p2.Lon)

	switch unit {
	case "m":
		return distMeters, nil
	case "km":
		return distMeters / 1000.0, nil
	case "mi":
		return distMeters * 0.000621371, nil
	case "ft":
		return distMeters * 3.28084, nil
	default:
		return distMeters, nil
	}
}

type GeoLocation struct {
	Name string
	Dist float64
	Lat  float64
	Lon  float64
}

func (d *DistributedMap) GeoRadius(key string, lat, lon, radius float64, unit string) ([]GeoLocation, error) {
	item, err := d.Get(key)
	if err != nil {
		return nil, nil
	}
	if item == nil {
		return nil, nil
	}

	item.Mu.RLock()
	defer item.Mu.RUnlock()

	if item.Type != TypeGeo {
		return nil, fmt.Errorf("WRONGTYPE")
	}

	// Convert radius to meters for check
	radiusM := radius
	switch unit {
	case "km":
		radiusM = radius * 1000.0
	case "mi":
		radiusM = radius * 1609.34
	case "ft":
		radiusM = radius * 0.3048
	}

	// Bounding box for grid iteration
	// 1 degree lat ~ 111km
	latRange := (radiusM / 1000.0) / 111.0
	// 1 degree lon ~ 111km * cos(lat)
	lonRange := (radiusM / 1000.0) / (111.0 * math.Cos(lat*math.Pi/180))

	minLat := lat - latRange
	maxLat := lat + latRange
	minLon := lon - lonRange
	maxLon := lon + lonRange

	// Add padding
	minLat -= gridLatStep
	maxLat += gridLatStep
	minLon -= gridLonStep
	maxLon += gridLonStep

	var results []GeoLocation

	// Iterate estimated grids
	minLatIdx := int(math.Floor(minLat / gridLatStep))
	maxLatIdx := int(math.Floor(maxLat / gridLatStep))
	minLonIdx := int(math.Floor(minLon / gridLonStep))
	maxLonIdx := int(math.Floor(maxLon / gridLonStep))

	checkedGrids := make(map[string]struct{})

	for i := minLatIdx; i <= maxLatIdx; i++ {
		for j := minLonIdx; j <= maxLonIdx; j++ {
			gKey := fmt.Sprintf("%d:%d", i, j)
			if _, done := checkedGrids[gKey]; done {
				continue
			}
			checkedGrids[gKey] = struct{}{}

			if members, ok := item.Geo.grid[gKey]; ok {
				for m := range members {
					p := item.Geo.members[m]
					dist := haversine(lat, lon, p.Lat, p.Lon)

					if dist <= radiusM {
						// Convert dist to requested unit
						resDist := dist
						switch unit {
						case "km":
							resDist = dist / 1000.0
						case "mi":
							resDist = dist * 0.000621371
						case "ft":
							resDist = dist * 3.28084
						}

						results = append(results, GeoLocation{
							Name: m,
							Dist: resDist,
							Lat:  p.Lat,
							Lon:  p.Lon,
						})
					}
				}
			}
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Dist < results[j].Dist
	})

	return results, nil
}
