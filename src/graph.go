package main

import (
	"container/heap"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/paulmach/osm"
)

type Edge struct {
	To          int
	DistM       float64
	WayID       int64
	RailType    string
	MaxSpeedKPH float64
}

type RailGraph struct {
	NodeIDs     []int64
	NodeCoords  [][2]float64 // index -> (lat, lon)
	Adj         [][]Edge
	IndexByNode map[int64]int

	Grid *GridIndex
}

type RouteOptions struct {
	K         int
	Algorithm string // "astar" or "dijkstra"
	// If true, include acceleration/deceleration penalties between speed changes.
	UseAccelDecel bool
}

type TopKOptions struct {
	K         int
	SnapK     int
	MaxPairs  int
	Algorithm string
	// If true, include acceleration/deceleration penalties between speed changes.
	UseAccelDecel bool
	// CorridorSimilarityThreshold controls how similar two routes can be before being
	// considered the "same corridor" and filtered out. Range: [0,1].
	// Higher values allow more similar alternatives; lower values enforce more diversity.
	CorridorSimilarityThreshold float64
	// SearchK controls how many candidates RouteK generates per snap-pair.
	// If zero, it is derived from K (over-generated) to improve chances of returning K results.
	SearchK int
}

type RouteStats struct {
	Visited int
	Relaxed int
}

type edgeKey struct {
	from int
	to   int
}

type routeCandidate struct {
	nodes   []int
	dist    float64
	ways    []int64
	stats   RouteStats
	key     string
	routeMs int64
}

type topKScored struct {
	key   string
	score float64
	res   *PathResult
	edges map[uint64]struct{}
}

type railGraphCache struct {
	NodeIDs    []int64
	NodeCoords [][2]float64
	Adj        [][]Edge
}

func LoadOrBuildGraph(osmPath string, nodesByID map[osm.NodeID]*osm.Node, lines map[osm.WayID]RailwayLine) (*RailGraph, error) {
	sig, err := fileSignature(osmPath)
	if err != nil {
		return nil, err
	}

	cacheDir := filepath.Join(filepath.Dir(osmPath), ".cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, err
	}
	cachePath := filepath.Join(cacheDir, fmt.Sprintf("railgraph-%s.gob", sig))

	start := time.Now()
	if g, err := loadGraphGob(cachePath); err == nil {
		log.Printf("graph cache hit (%s) in %s", filepath.Base(cachePath), time.Since(start).Truncate(time.Millisecond))
		return g, nil
	} else {
		log.Printf("graph cache miss (%s): %v", filepath.Base(cachePath), err)
	}

	log.Printf("building rail graph from %d ways (%d nodes seen)", len(lines), len(nodesByID))
	g := buildGraph(nodesByID, lines)
	log.Printf("built graph: %d nodes, %d adjacency lists in %s", len(g.NodeIDs), len(g.Adj), time.Since(start).Truncate(time.Millisecond))
	if err := saveGraphGob(cachePath, g); err != nil {
		log.Printf("warning: failed to save graph cache %s: %v", cachePath, err)
	} else {
		log.Printf("graph cache saved (%s)", filepath.Base(cachePath))
	}
	return g, nil
}

func loadGraphGob(path string) (*RailGraph, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var snap railGraphCache
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&snap); err != nil {
		return nil, err
	}
	g := RailGraph{
		NodeIDs:    snap.NodeIDs,
		NodeCoords: snap.NodeCoords,
		Adj:        snap.Adj,
	}
	// Rebuild derived fields.
	g.IndexByNode = make(map[int64]int, len(g.NodeIDs))
	for i, id := range g.NodeIDs {
		g.IndexByNode[id] = i
	}
	g.Grid = NewGridIndex(g.NodeCoords, defaultGridCellSizeDeg())
	return &g, nil
}

func saveGraphGob(path string, g *RailGraph) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	snap := railGraphCache{
		NodeIDs:    g.NodeIDs,
		NodeCoords: g.NodeCoords,
		Adj:        g.Adj,
	}
	if err := enc.Encode(snap); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func buildGraph(nodesByID map[osm.NodeID]*osm.Node, lines map[osm.WayID]RailwayLine) *RailGraph {
	// Collect all unique node IDs used by railway ways.
	used := make(map[int64]struct{}, 1_000_000)
	for _, ln := range lines {
		for _, nid := range ln.NodeIDs {
			used[int64(nid)] = struct{}{}
		}
	}

	ids := make([]int64, 0, len(used))
	for id := range used {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	indexByNode := make(map[int64]int, len(ids))
	coords := make([][2]float64, len(ids))
	for i, id := range ids {
		indexByNode[id] = i
		if n, ok := nodesByID[osm.NodeID(id)]; ok {
			coords[i] = [2]float64{n.Lat, n.Lon}
		} else {
			coords[i] = [2]float64{math.NaN(), math.NaN()}
		}
	}

	adj := make([][]Edge, len(ids))

	for wayID, ln := range lines {
		if len(ln.NodeIDs) < 2 {
			continue
		}
		maxSpeedKPH := wayRatedSpeedKPH(ln.Tags, ln.Type)
		for i := 0; i+1 < len(ln.NodeIDs); i++ {
			aID := int64(ln.NodeIDs[i])
			bID := int64(ln.NodeIDs[i+1])
			a, okA := indexByNode[aID]
			b, okB := indexByNode[bID]
			if !okA || !okB {
				continue
			}
			lat1, lon1 := coords[a][0], coords[a][1]
			lat2, lon2 := coords[b][0], coords[b][1]
			if math.IsNaN(lat1) || math.IsNaN(lat2) {
				continue
			}
			d := haversineMeters(lat1, lon1, lat2, lon2)
			if d <= 0 {
				continue
			}
			wid := int64(wayID)
			adj[a] = append(adj[a], Edge{To: b, DistM: d, WayID: wid, RailType: ln.Type, MaxSpeedKPH: maxSpeedKPH})
			adj[b] = append(adj[b], Edge{To: a, DistM: d, WayID: wid, RailType: ln.Type, MaxSpeedKPH: maxSpeedKPH})
		}
	}

	g := &RailGraph{
		NodeIDs:     ids,
		NodeCoords:  coords,
		Adj:         adj,
		IndexByNode: indexByNode,
	}
	g.Grid = NewGridIndex(g.NodeCoords, defaultGridCellSizeDeg())
	return g
}

func RouteStations(g *RailGraph, fromLatLon, toLatLon [2]float64, opt RouteOptions) (*PathResult, error) {
	t0 := time.Now()
	if opt.K <= 0 {
		opt.K = 6
	}
	algo := opt.Algorithm
	if algo == "" {
		algo = "astar"
	}
	useAccelDecel := opt.UseAccelDecel

	startCandidates := g.Grid.KNearest(fromLatLon[0], fromLatLon[1], opt.K)
	goalCandidates := g.Grid.KNearest(toLatLon[0], toLatLon[1], opt.K)
	if len(startCandidates) == 0 || len(goalCandidates) == 0 {
		return &PathResult{GenerationStatus: "no-snap", Timings: PathTimings{RouteMs: time.Since(t0).Milliseconds()}}, nil
	}

	type cand struct {
		idx  int
		snap float64
	}
	starts := make([]cand, 0, len(startCandidates))
	goals := make([]cand, 0, len(goalCandidates))
	for _, c := range startCandidates {
		starts = append(starts, cand{idx: c.Node, snap: c.DistM})
	}
	for _, c := range goalCandidates {
		goals = append(goals, cand{idx: c.Node, snap: c.DistM})
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].snap < starts[j].snap })
	sort.Slice(goals, func(i, j int) bool { return goals[i].snap < goals[j].snap })

	best := (*PathResult)(nil)
	bestCost := math.Inf(1)

	// Try a small cross-product of snap candidates (keeps it fast).
	maxPairs := minInt(36, len(starts)*len(goals))
	pairsTried := 0
	for i := 0; i < len(starts) && pairsTried < maxPairs; i++ {
		for j := 0; j < len(goals) && pairsTried < maxPairs; j++ {
			pairsTried++
			si := starts[i].idx
			gi := goals[j].idx

			pathIdx, distM, wayIDs, stats, ok := Route(g, si, gi, algo)
			if !ok {
				continue
			}
			total := distM + starts[i].snap + goals[j].snap
			if total >= bestCost {
				continue
			}

			coords := make([][2]float64, 0, len(pathIdx)+2)
			coords = append(coords, fromLatLon)
			for _, idx := range pathIdx {
				coords = append(coords, g.NodeCoords[idx])
			}
			coords = append(coords, toLatLon)

			bestCost = total
			estSec := estimateTravelSeconds(g, pathIdx, starts[i].snap, goals[j].snap, useAccelDecel)
			best = &PathResult{
				Coords:             coords,
				DistanceMeters:     total,
				EstimatedTravelSec: estSec,
				WayIDs:             wayIDs,
				SnapFromMeters:     starts[i].snap,
				SnapToMeters:       goals[j].snap,
				VisitedNodes:       stats.Visited,
				RelaxedEdges:       stats.Relaxed,
				Algorithm:          algo,
				GenerationStatus:   "ok",
			}
		}
	}

	if best == nil {
		return &PathResult{GenerationStatus: "no-route", Timings: PathTimings{RouteMs: time.Since(t0).Milliseconds()}}, nil
	}
	best.Timings = PathTimings{RouteMs: time.Since(t0).Milliseconds()}
	return best, nil
}

func RouteStationsTopK(g *RailGraph, fromLatLon, toLatLon [2]float64, opt TopKOptions) ([]*PathResult, error) {
	t0 := time.Now()
	desiredK := opt.K
	if desiredK <= 0 {
		desiredK = 3
	}
	snapK := opt.SnapK
	if snapK <= 0 {
		snapK = 10
	}
	maxPairs := opt.MaxPairs
	if maxPairs <= 0 {
		maxPairs = 220
	}
	algo := opt.Algorithm
	if algo == "" {
		algo = "astar"
	}
	useAccelDecel := opt.UseAccelDecel
	// Keep interactive use performant, but still search enough to actually fill desiredK.
	if desiredK <= 3 {
		if snapK > 8 {
			snapK = 8
		}
		if maxPairs > 60 {
			maxPairs = 60
		}
	}
	if desiredK <= 2 {
		if snapK > 6 {
			snapK = 6
		}
		if maxPairs > 40 {
			maxPairs = 40
		}
	}

	startCandidates := g.Grid.KNearest(fromLatLon[0], fromLatLon[1], snapK)
	goalCandidates := g.Grid.KNearest(toLatLon[0], toLatLon[1], snapK)
	if len(startCandidates) == 0 || len(goalCandidates) == 0 {
		return []*PathResult{{GenerationStatus: "no-snap", Timings: PathTimings{RouteMs: time.Since(t0).Milliseconds()}}}, nil
	}

	type cand struct {
		idx  int
		snap float64
	}
	starts := make([]cand, 0, len(startCandidates))
	goals := make([]cand, 0, len(goalCandidates))
	for _, c := range startCandidates {
		starts = append(starts, cand{idx: c.Node, snap: c.DistM})
	}
	for _, c := range goalCandidates {
		goals = append(goals, cand{idx: c.Node, snap: c.DistM})
	}
	sort.Slice(starts, func(i, j int) bool { return starts[i].snap < starts[j].snap })
	sort.Slice(goals, func(i, j int) bool { return goals[i].snap < goals[j].snap })

	best := make([]topKScored, 0, desiredK)
	fallback := make([]topKScored, 0, desiredK*10)
	seen := make(map[string]struct{}, desiredK*48)
	corridorSimilarityThreshold := opt.CorridorSimilarityThreshold
	if corridorSimilarityThreshold <= 0 {
		corridorSimilarityThreshold = 0.82
	}
	if corridorSimilarityThreshold < 0 {
		corridorSimilarityThreshold = 0
	}
	if corridorSimilarityThreshold > 1 {
		corridorSimilarityThreshold = 1
	}

	searchK := opt.SearchK
	if searchK <= 0 {
		searchK = desiredK * 5
	}
	if searchK < desiredK {
		searchK = desiredK
	}
	// Guardrail: prevent pathological blow-ups.
	if searchK > 40 {
		searchK = 40
	}

	pairsTried := 0
searchPairs:
	for i := 0; i < len(starts) && pairsTried < maxPairs; i++ {
		for j := 0; j < len(goals) && pairsTried < maxPairs; j++ {
			pairsTried++
			si := starts[i].idx
			gi := goals[j].idx

			paths, err := RouteK(g, si, gi, algo, searchK)
			if err != nil || len(paths) == 0 {
				continue
			}
			for _, path := range paths {
				total := path.dist + starts[i].snap + goals[j].snap
				dirPenalty := directionalPenaltyMeters(g, path.nodes, fromLatLon, toLatLon)
				score := total + dirPenalty

				key := path.key
				if _, ok := seen[key]; ok {
					continue
				}
				candEdges := pathEdgeSet(path.nodes)
				seen[key] = struct{}{}

				coords := make([][2]float64, 0, len(path.nodes)+2)
				coords = append(coords, fromLatLon)
				for _, idx := range path.nodes {
					coords = append(coords, g.NodeCoords[idx])
				}
				coords = append(coords, toLatLon)

				pr := &PathResult{
					Coords:             coords,
					DistanceMeters:     total,
					EstimatedTravelSec: estimateTravelSeconds(g, path.nodes, starts[i].snap, goals[j].snap, useAccelDecel),
					WayIDs:             path.ways,
					SnapFromMeters:     starts[i].snap,
					SnapToMeters:       goals[j].snap,
					VisitedNodes:       path.stats.Visited,
					RelaxedEdges:       path.stats.Relaxed,
					Algorithm:          algo,
					GenerationStatus:   "ok",
					Timings:            PathTimings{RouteMs: path.routeMs},
				}

				cand := topKScored{key: key, score: score, res: pr, edges: candEdges}
				if isSameCorridor(candEdges, best, corridorSimilarityThreshold) {
					fallback = append(fallback, cand)
					continue
				}
				best = append(best, cand)
				sort.Slice(best, func(a, b int) bool { return best[a].score < best[b].score })
				if len(best) > desiredK {
					// Keep strongest diverse results; extras stay available for backfill.
					fallback = append(fallback, best[len(best)-1])
					best = best[:desiredK]
				}
			}
			if len(best) >= desiredK && desiredK <= 2 {
				break searchPairs
			}
		}
	}

	if len(best) == 0 {
		return []*PathResult{{GenerationStatus: "no-route", Timings: PathTimings{RouteMs: time.Since(t0).Milliseconds()}}}, nil
	}
	if len(best) < desiredK && len(fallback) > 0 {
		sort.Slice(fallback, func(i, j int) bool { return fallback[i].score < fallback[j].score })
		for _, c := range fallback {
			if len(best) >= desiredK {
				break
			}
			already := false
			for _, b := range best {
				if b.key == c.key {
					already = true
					break
				}
			}
			if already {
				continue
			}
			// Preserve corridor diversity even during backfill.
			// If no additional distinct corridors exist, we'll relax and fill from fallback below.
			if isSameCorridor(c.edges, best, corridorSimilarityThreshold) {
				continue
			}
			best = append(best, c)
		}
		sort.Slice(best, func(i, j int) bool { return best[i].score < best[j].score })
	}
	// Last resort: if we still don't have desiredK, fill with next-best remaining candidates
	// even if they overlap the corridor threshold. This ensures opt.K represents the number
	// of paths returned, not the search effort.
	if len(best) < desiredK && len(fallback) > 0 {
		for _, c := range fallback {
			if len(best) >= desiredK {
				break
			}
			already := false
			for _, b := range best {
				if b.key == c.key {
					already = true
					break
				}
			}
			if already {
				continue
			}
			best = append(best, c)
		}
		sort.Slice(best, func(i, j int) bool { return best[i].score < best[j].score })
		if len(best) > desiredK {
			best = best[:desiredK]
		}
	}
	out := make([]*PathResult, 0, len(best))
	for _, s := range best {
		out = append(out, s.res)
	}
	return out, nil
}

func wayKey(wayIDs []int64) string {
	if len(wayIDs) == 0 {
		return "none"
	}
	// stable and fast enough for small lists.
	b := make([]byte, 0, len(wayIDs)*10)
	for i, w := range wayIDs {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, []byte(fmt.Sprintf("%d", w))...)
	}
	return string(b)
}

func Route(g *RailGraph, start, goal int, algo string) ([]int, float64, []int64, RouteStats, bool) {
	return RouteWithConstraints(g, start, goal, algo, nil, nil)
}

func RouteWithConstraints(
	g *RailGraph,
	start,
	goal int,
	algo string,
	bannedEdges map[edgeKey]struct{},
	bannedNodes map[int]struct{},
) ([]int, float64, []int64, RouteStats, bool) {
	switch algo {
	case "dijkstra":
		return dijkstraWithConstraints(g, start, goal, bannedEdges, bannedNodes)
	default:
		return astarWithConstraints(g, start, goal, bannedEdges, bannedNodes)
	}
}

func RouteK(g *RailGraph, start, goal int, algo string, k int) ([]routeCandidate, error) {
	k = maxInt(1, k)
	t0 := time.Now()
	firstNodes, firstDist, firstWays, firstStats, ok := RouteWithConstraints(g, start, goal, algo, nil, nil)
	if !ok {
		return nil, nil
	}

	best := []routeCandidate{{
		nodes:   firstNodes,
		dist:    firstDist,
		ways:    firstWays,
		stats:   firstStats,
		key:     wayKey(firstWays),
		routeMs: time.Since(t0).Milliseconds(),
	}}
	used := map[string]struct{}{best[0].key: {}}
	if k == 1 {
		return best, nil
	}

	// 1) Corridor-span bans from baseline route.
	for _, span := range corridorSpansFromPath(g, firstNodes, minInt(6, k*3)) {
		if len(best) >= k {
			break
		}
		bannedEdges := make(map[edgeKey]struct{}, len(span.edges)*2)
		for _, e := range span.edges {
			bannedEdges[e] = struct{}{}
			bannedEdges[edgeKey{from: e.to, to: e.from}] = struct{}{}
		}
		rt0 := time.Now()
		altNodes, altDist, altWays, altStats, altOk := RouteWithConstraints(g, start, goal, algo, bannedEdges, nil)
		if !altOk || len(altNodes) == 0 {
			continue
		}
		candidate := routeCandidate{
			nodes:   altNodes,
			dist:    altDist,
			ways:    altWays,
			stats:   altStats,
			key:     wayKey(altWays),
			routeMs: time.Since(rt0).Milliseconds(),
		}
		if _, exists := used[candidate.key]; exists {
			continue
		}
		used[candidate.key] = struct{}{}
		best = append(best, candidate)
	}

	// 2) If still missing alternatives, perturb windows on current best path.
	if len(best) < k {
		for _, window := range diversionWindows(firstNodes, minInt(8, k*4)) {
			if len(best) >= k {
				break
			}
			bannedEdges := make(map[edgeKey]struct{}, (window.to-window.from+1)*2)
			for i := window.from; i < window.to && i+1 < len(firstNodes); i++ {
				u := firstNodes[i]
				v := firstNodes[i+1]
				bannedEdges[edgeKey{from: u, to: v}] = struct{}{}
				bannedEdges[edgeKey{from: v, to: u}] = struct{}{}
			}
			rt0 := time.Now()
			altNodes, altDist, altWays, altStats, altOk := RouteWithConstraints(g, start, goal, algo, bannedEdges, nil)
			if !altOk || len(altNodes) == 0 {
				continue
			}
			candidate := routeCandidate{
				nodes:   altNodes,
				dist:    altDist,
				ways:    altWays,
				stats:   altStats,
				key:     wayKey(altWays),
				routeMs: time.Since(rt0).Milliseconds(),
			}
			if _, exists := used[candidate.key]; exists {
				continue
			}
			used[candidate.key] = struct{}{}
			best = append(best, candidate)
		}
	}
	sort.Slice(best, func(i, j int) bool { return best[i].dist < best[j].dist })
	if len(best) > k {
		best = best[:k]
	}
	return best, nil
}

func astar(g *RailGraph, start, goal int) ([]int, float64, []int64, RouteStats, bool) {
	return astarWithConstraints(g, start, goal, nil, nil)
}

func astarWithConstraints(g *RailGraph, start, goal int, bannedEdges map[edgeKey]struct{}, bannedNodes map[int]struct{}) ([]int, float64, []int64, RouteStats, bool) {
	n := len(g.NodeCoords)
	dist := make([]float64, n)
	prev := make([]int, n)
	prevWay := make([]int64, n)
	for i := 0; i < n; i++ {
		dist[i] = math.Inf(1)
		prev[i] = -1
	}

	dist[start] = 0
	pq := &nodePQ{}
	heap.Init(pq)
	heap.Push(pq, &pqItem{Node: start, Priority: heuristicM(g, start, goal)})
	if _, blocked := bannedNodes[start]; blocked {
		return nil, 0, nil, RouteStats{}, false
	}

	visited := make([]bool, n)
	stats := RouteStats{}

	for pq.Len() > 0 {
		it := heap.Pop(pq).(*pqItem)
		u := it.Node
		if _, blocked := bannedNodes[u]; blocked {
			continue
		}
		if visited[u] {
			continue
		}
		visited[u] = true
		stats.Visited++
		if u == goal {
			break
		}

		for _, e := range g.Adj[u] {
			if bannedEdges != nil {
				if _, blocked := bannedEdges[edgeKey{from: u, to: e.To}]; blocked {
					continue
				}
			}
			if _, blocked := bannedNodes[e.To]; blocked {
				continue
			}
			stats.Relaxed++
			nd := dist[u] + e.DistM
			if nd < dist[e.To] {
				dist[e.To] = nd
				prev[e.To] = u
				prevWay[e.To] = e.WayID
				heap.Push(pq, &pqItem{Node: e.To, Priority: nd + heuristicM(g, e.To, goal)})
			}
		}
	}

	if math.IsInf(dist[goal], 1) {
		return nil, 0, nil, stats, false
	}
	path := reconstructPath(prev, start, goal)
	ways := reconstructWays(prevWay, path)
	return path, dist[goal], ways, stats, true
}

func dijkstra(g *RailGraph, start, goal int) ([]int, float64, []int64, RouteStats, bool) {
	return dijkstraWithConstraints(g, start, goal, nil, nil)
}

func dijkstraWithConstraints(g *RailGraph, start, goal int, bannedEdges map[edgeKey]struct{}, bannedNodes map[int]struct{}) ([]int, float64, []int64, RouteStats, bool) {
	n := len(g.NodeCoords)
	dist := make([]float64, n)
	prev := make([]int, n)
	prevWay := make([]int64, n)
	for i := 0; i < n; i++ {
		dist[i] = math.Inf(1)
		prev[i] = -1
	}

	dist[start] = 0
	pq := &nodePQ{}
	heap.Init(pq)
	heap.Push(pq, &pqItem{Node: start, Priority: 0})
	if _, blocked := bannedNodes[start]; blocked {
		return nil, 0, nil, RouteStats{}, false
	}

	visited := make([]bool, n)
	stats := RouteStats{}

	for pq.Len() > 0 {
		it := heap.Pop(pq).(*pqItem)
		u := it.Node
		if _, blocked := bannedNodes[u]; blocked {
			continue
		}
		if visited[u] {
			continue
		}
		visited[u] = true
		stats.Visited++
		if u == goal {
			break
		}
		for _, e := range g.Adj[u] {
			if bannedEdges != nil {
				if _, blocked := bannedEdges[edgeKey{from: u, to: e.To}]; blocked {
					continue
				}
			}
			if _, blocked := bannedNodes[e.To]; blocked {
				continue
			}
			stats.Relaxed++
			nd := dist[u] + e.DistM
			if nd < dist[e.To] {
				dist[e.To] = nd
				prev[e.To] = u
				prevWay[e.To] = e.WayID
				heap.Push(pq, &pqItem{Node: e.To, Priority: nd})
			}
		}
	}

	if math.IsInf(dist[goal], 1) {
		return nil, 0, nil, stats, false
	}
	path := reconstructPath(prev, start, goal)
	ways := reconstructWays(prevWay, path)
	return path, dist[goal], ways, stats, true
}

func reconstructPath(prev []int, start, goal int) []int {
	// Returns node indices (includes start and goal).
	out := make([]int, 0, 256)
	for cur := goal; cur != -1; cur = prev[cur] {
		out = append(out, cur)
		if cur == start {
			break
		}
	}
	// reverse
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func reconstructWays(prevWay []int64, path []int) []int64 {
	if len(path) < 2 {
		return nil
	}
	ways := make([]int64, 0, len(path))
	var last int64
	for i := 1; i < len(path); i++ {
		wid := prevWay[path[i]]
		if wid == 0 {
			continue
		}
		if wid == last {
			continue
		}
		ways = append(ways, wid)
		last = wid
	}
	return ways
}

func estimateTravelSeconds(g *RailGraph, nodes []int, snapFromMeters, snapToMeters float64, useAccelDecel bool) float64 {
	if len(nodes) < 2 {
		return 0
	}
	totalSec := 0.0
	startSpeed := 0.0
	endSpeed := 0.0
	prevSpeedKPH := 0.0
	transitionSec := 0.0
	havePrevSpeed := false
	for i := 0; i+1 < len(nodes); i++ {
		e, ok := bestEdgeBetween(g, nodes[i], nodes[i+1])
		if !ok {
			continue
		}
		speedKPH := ratedSpeedKPH(e.MaxSpeedKPH, e.RailType)
		if speedKPH <= 0 {
			continue
		}
		if i == 0 {
			startSpeed = speedKPH
		}
		if havePrevSpeed {
			transitionSec += speedTransitionSeconds(prevSpeedKPH, speedKPH)
		}
		prevSpeedKPH = speedKPH
		havePrevSpeed = true
		endSpeed = speedKPH
		totalSec += e.DistM / (speedKPH * 1000 / 3600)
	}
	if useAccelDecel {
		totalSec += transitionSec
	}
	terminalSpeed := (startSpeed + endSpeed) * 0.5
	if terminalSpeed <= 0 {
		terminalSpeed = 60
	}
	totalSec += (snapFromMeters + snapToMeters) / (terminalSpeed * 1000 / 3600)
	return totalSec
}

func speedTransitionSeconds(fromKPH, toKPH float64) float64 {
	if fromKPH <= 0 || toKPH <= 0 {
		return 0
	}
	const accelMPS2 = 0.45
	const decelMPS2 = 0.60
	const minDeltaKPH = 1.0
	deltaKPH := toKPH - fromKPH
	if math.Abs(deltaKPH) < minDeltaKPH {
		return 0
	}
	deltaMPS := math.Abs(deltaKPH) / 3.6
	if deltaKPH > 0 {
		return deltaMPS / accelMPS2
	}
	return deltaMPS / decelMPS2
}

func bestEdgeBetween(g *RailGraph, from, to int) (Edge, bool) {
	if from < 0 || from >= len(g.Adj) {
		return Edge{}, false
	}
	bestDist := math.Inf(1)
	var best Edge
	found := false
	for _, e := range g.Adj[from] {
		if e.To != to {
			continue
		}
		if e.DistM < bestDist {
			bestDist = e.DistM
			best = e
			found = true
		}
	}
	return best, found
}

func ratedSpeedKPH(maxSpeedKPH float64, railType string) float64 {
	if maxSpeedKPH > 0 {
		return maxSpeedKPH
	}
	switch railType {
	case "high_speed":
		return 250
	case "rail":
		return 120
	case "light_rail":
		return 90
	case "subway":
		return 80
	case "tram":
		return 50
	case "monorail":
		return 70
	case "narrow_gauge":
		return 60
	default:
		return 100
	}
}

func wayRatedSpeedKPH(tags osm.Tags, railType string) float64 {
	if tags == nil {
		return ratedSpeedKPH(0, railType)
	}
	keys := []string{
		"maxspeed",
		"maxspeed:railway",
		"railway:maxspeed",
		"maxspeed:forward",
		"maxspeed:backward",
	}
	best := 0.0
	for _, key := range keys {
		v := strings.TrimSpace(tags.Find(key))
		if v == "" {
			continue
		}
		sp := parseOSMSpeedKPH(v)
		if sp > best {
			best = sp
		}
	}
	return ratedSpeedKPH(best, railType)
}

func parseOSMSpeedKPH(raw string) float64 {
	v := strings.ToLower(strings.TrimSpace(raw))
	if v == "" {
		return 0
	}
	// OSM can store combined values, e.g. "120;100 mph".
	parts := strings.Split(v, ";")
	best := 0.0
	for _, p := range parts {
		token := strings.TrimSpace(strings.ReplaceAll(p, ",", "."))
		if token == "" {
			continue
		}
		mult := 1.0 // kph
		if strings.Contains(token, "mph") {
			mult = 1.609344
			token = strings.ReplaceAll(token, "mph", "")
		}
		token = strings.ReplaceAll(token, "km/h", "")
		token = strings.ReplaceAll(token, "kmh", "")
		token = strings.ReplaceAll(token, "kph", "")
		token = strings.TrimSpace(token)
		fields := strings.Fields(token)
		if len(fields) > 0 {
			token = fields[0]
		}
		num, err := strconv.ParseFloat(token, 64)
		if err != nil || num <= 0 {
			continue
		}
		kph := num * mult
		if kph > best {
			best = kph
		}
	}
	return best
}

func heuristicM(g *RailGraph, a, b int) float64 {
	la, lo := g.NodeCoords[a][0], g.NodeCoords[a][1]
	lb, lob := g.NodeCoords[b][0], g.NodeCoords[b][1]
	return haversineMeters(la, lo, lb, lob)
}

type pqItem struct {
	Node     int
	Priority float64
	Index    int
}

type nodePQ []*pqItem

func (p nodePQ) Len() int           { return len(p) }
func (p nodePQ) Less(i, j int) bool { return p[i].Priority < p[j].Priority }
func (p nodePQ) Swap(i, j int)      { p[i], p[j] = p[j], p[i]; p[i].Index, p[j].Index = i, j }
func (p *nodePQ) Push(x any)        { *p = append(*p, x.(*pqItem)) }
func (p *nodePQ) Pop() any          { old := *p; n := len(old); it := old[n-1]; *p = old[:n-1]; return it }

func haversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	const R = 6371000.0
	φ1 := lat1 * math.Pi / 180
	φ2 := lat2 * math.Pi / 180
	dφ := (lat2 - lat1) * math.Pi / 180
	dλ := (lon2 - lon1) * math.Pi / 180
	sinDφ := math.Sin(dφ / 2)
	sinDλ := math.Sin(dλ / 2)
	a := sinDφ*sinDφ + math.Cos(φ1)*math.Cos(φ2)*sinDλ*sinDλ
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return R * c
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func hasNodePrefix(path []int, prefix []int) bool {
	if len(prefix) > len(path) {
		return false
	}
	for i := range prefix {
		if path[i] != prefix[i] {
			return false
		}
	}
	return true
}

func pathPrefixDistances(g *RailGraph, nodes []int) ([]float64, bool) {
	if len(nodes) == 0 {
		return nil, false
	}
	out := make([]float64, len(nodes))
	for i := 1; i < len(nodes); i++ {
		d, ok := edgeDistance(g, nodes[i-1], nodes[i])
		if !ok {
			return nil, false
		}
		out[i] = out[i-1] + d
	}
	return out, true
}

func edgeDistance(g *RailGraph, from, to int) (float64, bool) {
	if from < 0 || from >= len(g.Adj) {
		return 0, false
	}
	best := math.Inf(1)
	for _, e := range g.Adj[from] {
		if e.To == to && e.DistM < best {
			best = e.DistM
		}
	}
	if math.IsInf(best, 1) {
		return 0, false
	}
	return best, true
}

func pathToWayIDs(g *RailGraph, nodes []int) []int64 {
	if len(nodes) < 2 {
		return nil
	}
	ways := make([]int64, 0, len(nodes))
	var last int64
	for i := 1; i < len(nodes); i++ {
		best := int64(0)
		bestDist := math.Inf(1)
		u := nodes[i-1]
		v := nodes[i]
		for _, e := range g.Adj[u] {
			if e.To != v {
				continue
			}
			if e.DistM < bestDist {
				bestDist = e.DistM
				best = e.WayID
			}
		}
		if best == 0 {
			continue
		}
		if best == last {
			continue
		}
		ways = append(ways, best)
		last = best
	}
	return ways
}

type corridorSpan struct {
	wayID int64
	edges []edgeKey
}

func corridorSpansFromPath(g *RailGraph, nodes []int, maxSpans int) []corridorSpan {
	if len(nodes) < 2 {
		return nil
	}
	if maxSpans <= 0 {
		maxSpans = 1
	}

	seen := make(map[int64]struct{}, maxSpans)
	spans := make([]corridorSpan, 0, minInt(len(nodes)/8+1, maxSpans))

	for i := 0; i+1 < len(nodes); i++ {
		wayID, ok := pathEdgeWayID(g, nodes[i], nodes[i+1])
		if !ok {
			continue
		}
		if len(spans) == 0 || wayID != spans[len(spans)-1].wayID {
			if _, exists := seen[wayID]; exists {
				continue
			}
			seen[wayID] = struct{}{}
			spans = append(spans, corridorSpan{wayID: wayID})
			if len(spans) >= maxSpans {
				break
			}
		}
		last := len(spans) - 1
		spans[last].edges = append(spans[last].edges, edgeKey{from: nodes[i], to: nodes[i+1]})
	}

	return spans
}

func pathEdgeWayID(g *RailGraph, from, to int) (int64, bool) {
	if from < 0 || from >= len(g.Adj) {
		return 0, false
	}
	best := int64(0)
	bestDist := math.Inf(1)
	for _, e := range g.Adj[from] {
		if e.To != to {
			continue
		}
		if e.DistM < bestDist {
			bestDist = e.DistM
			best = e.WayID
		}
	}
	if best == 0 || math.IsInf(bestDist, 1) {
		return 0, false
	}
	return best, true
}

type diversionWindow struct {
	from int
	to   int
}

func diversionWindows(nodes []int, maxWindows int) []diversionWindow {
	if len(nodes) < 8 || maxWindows <= 0 {
		return nil
	}
	edgeCount := len(nodes) - 1
	minLen := maxInt(2, edgeCount/8)
	maxLen := maxInt(minLen, edgeCount/3)

	centers := []float64{0.33, 0.50, 0.67, 0.25, 0.75}
	out := make([]diversionWindow, 0, minInt(maxWindows, len(centers)*2))
	seen := map[diversionWindow]struct{}{}
	for _, c := range centers {
		center := int(float64(edgeCount) * c)
		for _, length := range []int{minLen, maxLen} {
			from := center - length/2
			if from < 0 {
				from = 0
			}
			to := from + length
			if to > edgeCount {
				to = edgeCount
				from = maxInt(0, to-length)
			}
			w := diversionWindow{from: from, to: to}
			if w.to-w.from < 2 {
				continue
			}
			if _, ok := seen[w]; ok {
				continue
			}
			seen[w] = struct{}{}
			out = append(out, w)
			if len(out) >= maxWindows {
				return out
			}
		}
	}
	return out
}

func pathEdgeSet(nodes []int) map[uint64]struct{} {
	if len(nodes) < 2 {
		return nil
	}
	out := make(map[uint64]struct{}, len(nodes)-1)
	for i := 0; i+1 < len(nodes); i++ {
		a := nodes[i]
		b := nodes[i+1]
		if a > b {
			a, b = b, a
		}
		key := (uint64(uint32(a)) << 32) | uint64(uint32(b))
		out[key] = struct{}{}
	}
	return out
}

func edgeOverlapRatio(a, b map[uint64]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	shorter := a
	longer := b
	if len(a) > len(b) {
		shorter = b
		longer = a
	}
	shared := 0
	for e := range shorter {
		if _, ok := longer[e]; ok {
			shared++
		}
	}
	return float64(shared) / float64(len(shorter))
}

func isSameCorridor(candidate map[uint64]struct{}, best []topKScored, threshold float64) bool {
	for _, b := range best {
		if edgeOverlapRatio(candidate, b.edges) >= threshold {
			return true
		}
	}
	return false
}

func directionalPenaltyMeters(g *RailGraph, nodes []int, fromLatLon, toLatLon [2]float64) float64 {
	if len(nodes) < 2 {
		return 0
	}
	ux, uy, ok := tripDirectionUnit(fromLatLon, toLatLon)
	if !ok {
		return 0
	}
	backwardMeters := 0.0
	perpMeters := 0.0
	for i := 0; i+1 < len(nodes); i++ {
		a := g.NodeCoords[nodes[i]]
		b := g.NodeCoords[nodes[i+1]]
		vx, vy := approxPlanarDeltaMeters(a[0], a[1], b[0], b[1])
		segLen := math.Hypot(vx, vy)
		if segLen <= 0 {
			continue
		}
		proj := (vx*ux + vy*uy) / segLen // cosine alignment with trip direction
		if proj < 0 {
			backwardMeters += segLen * (-proj)
		}
		// Purely lateral movement is often a sign of same-corridor drift for alternatives.
		perpMeters += segLen * math.Sqrt(maxFloat(0, 1-proj*proj))
	}
	// Strongly punish moving opposite to destination; lightly punish lateral drift.
	return backwardMeters*3.0 + perpMeters*0.10
}

func tripDirectionUnit(fromLatLon, toLatLon [2]float64) (float64, float64, bool) {
	vx, vy := approxPlanarDeltaMeters(fromLatLon[0], fromLatLon[1], toLatLon[0], toLatLon[1])
	n := math.Hypot(vx, vy)
	if n == 0 {
		return 0, 0, false
	}
	return vx / n, vy / n, true
}

func approxPlanarDeltaMeters(lat1, lon1, lat2, lon2 float64) (float64, float64) {
	const metersPerDegLat = 111_320.0
	midLat := (lat1 + lat2) * 0.5 * math.Pi / 180
	metersPerDegLon := metersPerDegLat * math.Cos(midLat)
	dx := (lon2 - lon1) * metersPerDegLon
	dy := (lat2 - lat1) * metersPerDegLat
	return dx, dy
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
