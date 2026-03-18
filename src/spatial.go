package main

import (
	"math"
	"sort"
)

type GridIndex struct {
	cellDeg float64
	cells   map[int64][]int // cellKey -> node indices
	coords  [][2]float64
	seen    []uint32
	seenID  uint32
}

type Nearest struct {
	Node  int
	DistM float64
}

func defaultGridCellSizeDeg() float64 {
	// ~1.1km at equator; good tradeoff for large rail networks.
	return 0.01
}

func NewGridIndex(coords [][2]float64, cellDeg float64) *GridIndex {
	if cellDeg <= 0 {
		cellDeg = defaultGridCellSizeDeg()
	}
	g := &GridIndex{
		cellDeg: cellDeg,
		cells:   make(map[int64][]int, len(coords)/4+1),
		coords:  coords,
		seen:    make([]uint32, len(coords)),
	}
	for i, c := range coords {
		if math.IsNaN(c[0]) || math.IsNaN(c[1]) {
			continue
		}
		key := g.keyFor(c[0], c[1])
		g.cells[key] = append(g.cells[key], i)
	}
	return g
}

func (g *GridIndex) KNearest(lat, lon float64, k int) []Nearest {
	if k <= 0 {
		return nil
	}
	if len(g.coords) == 0 {
		return nil
	}

	cx, cy := g.cellXY(lat, lon)
	out := make([]Nearest, 0, k*3)

	g.seenID++
	if g.seenID == 0 {
		for i := range g.seen {
			g.seen[i] = 0
		}
		g.seenID = 1
	}

	// Expand ring by ring of cells until we have enough candidates.
	maxR := 12 // hard cap for worst-case; keeps snapping bounded.
	for r := 0; r <= maxR; r++ {
		for dx := -r; dx <= r; dx++ {
			for dy := -r; dy <= r; dy++ {
				if absInt(dx) != r && absInt(dy) != r {
					continue // only perimeter of the square ring
				}
				key := packCell(cx+dx, cy+dy)
				nodes := g.cells[key]
				for _, idx := range nodes {
					if g.seen[idx] == g.seenID {
						continue
					}
					g.seen[idx] = g.seenID
					c := g.coords[idx]
					d := haversineMeters(lat, lon, c[0], c[1])
					out = append(out, Nearest{Node: idx, DistM: d})
				}
			}
		}
		if len(out) >= k*2 {
			break
		}
	}

	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DistM < out[j].DistM })
	if len(out) > k {
		out = out[:k]
	}
	return out
}

func (g *GridIndex) cellXY(lat, lon float64) (int, int) {
	x := int(math.Floor(lon / g.cellDeg))
	y := int(math.Floor(lat / g.cellDeg))
	return x, y
}

func (g *GridIndex) keyFor(lat, lon float64) int64 {
	x, y := g.cellXY(lat, lon)
	return packCell(x, y)
}

func packCell(x, y int) int64 {
	// pack two signed 32-bit ints into one int64
	return (int64(x) << 32) ^ (int64(y) & 0xffffffff)
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
