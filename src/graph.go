package main

import (
	"container/heap"
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/paulmach/osm"
)

const (
	railTypeUnknown uint8 = iota
	railTypeHighSpeed
	railTypeRail
	railTypeLightRail
	railTypeSubway
	railTypeTram
	railTypeMonorail
	railTypeNarrowGauge
)

func railTypeFromString(s string) uint8 {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "high_speed":
		return railTypeHighSpeed
	case "rail":
		return railTypeRail
	case "light_rail":
		return railTypeLightRail
	case "subway":
		return railTypeSubway
	case "tram":
		return railTypeTram
	case "monorail":
		return railTypeMonorail
	case "narrow_gauge":
		return railTypeNarrowGauge
	default:
		return railTypeUnknown
	}
}

type Edge struct {
	To          int
	DistM       float32
	WayID       int64
	RailType    uint8
	MaxSpeedKPH float32
}

type RailGraph struct {
	NodeIDs     []int64
	NodeCoords  [][2]float32 // index -> (lat, lon)
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
	// Optional soft target travel time (seconds). When > 0, RouteStationsTopK will
	// lightly bias scoring toward candidates whose estimated travel duration is
	// closer to this value, especially avoiding unrealistically short routes.
	TargetTravelSec float64
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
	path    []routeEdgeMeta
}

type topKScored struct {
	key   string
	score float64
	res   *PathResult
	edges map[uint64]struct{}
}

type routeEdgeMeta struct {
	wayID       int64
	railType    uint8
	maxSpeedKPH float32
	distM       float32
}

type routeSearchState struct {
	dist      []float64
	prev      []int
	prevEdge  []routeEdgeMeta
	nodeStamp []uint32
	visStamp  []uint32
	pq        nodePQ
	epoch     uint32
}

var routeSearchStatePool = sync.Pool{
	New: func() any {
		return &routeSearchState{}
	},
}

type routeKCacheKey struct {
	start int
	goal  int
	k     int
	algo  string
}

type routeKCacheEntry struct {
	paths []routeCandidate
}

const routeKCacheMaxEntries = 256

var routeKCache = struct {
	mu    sync.RWMutex
	data  map[routeKCacheKey]routeKCacheEntry
	order []routeKCacheKey
}{
	data:  make(map[routeKCacheKey]routeKCacheEntry, routeKCacheMaxEntries),
	order: make([]routeKCacheKey, 0, routeKCacheMaxEntries),
}

func getRouteKFromCache(key routeKCacheKey) ([]routeCandidate, bool) {
	routeKCache.mu.RLock()
	entry, ok := routeKCache.data[key]
	routeKCache.mu.RUnlock()
	if !ok {
		return nil, false
	}
	// paths are treated as immutable by callers, so we can return as-is.
	return entry.paths, true
}

func putRouteKInCache(key routeKCacheKey, paths []routeCandidate) {
	if len(paths) == 0 {
		return
	}
	routeKCache.mu.Lock()
	defer routeKCache.mu.Unlock()
	if _, exists := routeKCache.data[key]; exists {
		return
	}
	// Evict oldest if at capacity.
	if len(routeKCache.order) >= routeKCacheMaxEntries {
		oldest := routeKCache.order[0]
		routeKCache.order = routeKCache.order[1:]
		delete(routeKCache.data, oldest)
	}
	routeKCache.data[key] = routeKCacheEntry{paths: paths}
	routeKCache.order = append(routeKCache.order, key)
}

type railGraphCache struct {
	NodeIDs    []int64
	NodeCoords [][2]float32
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
	coords := make([][2]float32, len(ids))
	for i, id := range ids {
		indexByNode[id] = i
		if n, ok := nodesByID[osm.NodeID(id)]; ok {
			coords[i] = [2]float32{float32(n.Lat), float32(n.Lon)}
		} else {
			coords[i] = [2]float32{float32(math.NaN()), float32(math.NaN())}
		}
	}

	adj := make([][]Edge, len(ids))

	for wayID, ln := range lines {
		if len(ln.NodeIDs) < 2 {
			continue
		}
		maxSpeedKPH := ln.MaxSpeedKPH
		rt := railTypeFromString(ln.Type)
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
			if math.IsNaN(float64(lat1)) || math.IsNaN(float64(lat2)) {
				continue
			}
			d := haversineMeters(float64(lat1), float64(lon1), float64(lat2), float64(lon2))
			if d <= 0 {
				continue
			}
			wid := int64(wayID)
			adj[a] = append(adj[a], Edge{To: b, DistM: float32(d), WayID: wid, RailType: rt, MaxSpeedKPH: maxSpeedKPH})
			adj[b] = append(adj[b], Edge{To: a, DistM: float32(d), WayID: wid, RailType: rt, MaxSpeedKPH: maxSpeedKPH})
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

			pathIdx, distM, wayIDs, pathEdges, stats, ok := Route(g, si, gi, algo)
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
					c := g.NodeCoords[idx]
					coords = append(coords, [2]float64{float64(c[0]), float64(c[1])})
				}
			coords = append(coords, toLatLon)

			bestCost = total
			estSec := estimateTravelSeconds(pathEdges, starts[i].snap, goals[j].snap, useAccelDecel)
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

func RouteStationsTopK(g *RailGraph, fromLatLon, toLatLon [2]float64, opt TopKOptions) ([]*PathResult, RoutePhaseTimings, error) {
	t0 := time.Now()
	phases := RoutePhaseTimings{}
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

	snapStart := time.Now()
	startCandidates := g.Grid.KNearest(fromLatLon[0], fromLatLon[1], snapK)
	goalCandidates := g.Grid.KNearest(toLatLon[0], toLatLon[1], snapK)
	phases.KNearestMs = time.Since(snapStart).Milliseconds()
	phases.CandidatesFound = len(startCandidates) + len(goalCandidates)
	if len(startCandidates) == 0 || len(goalCandidates) == 0 {
		phases.TotalMs = time.Since(t0).Milliseconds()
		return []*PathResult{{GenerationStatus: "no-snap", Timings: PathTimings{RouteMs: phases.TotalMs, Phases: &phases}}}, phases, nil
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

	pairTimingStart := time.Now()
	type pair struct {
		i int
		j int
	}
	type pairResult struct {
		i         int
		j         int
		paths     []routeCandidate
		elapsedMs int64
	}

	pairCh := make(chan pair)
	resultCh := make(chan pairResult)

	workerCount := runtime.GOMAXPROCS(0)
	if workerCount > 8 {
		workerCount = 8
	}
	if workerCount < 1 {
		workerCount = 1
	}

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for w := 0; w < workerCount; w++ {
		go func() {
			defer wg.Done()
			for p := range pairCh {
				si := starts[p.i].idx
				gi := goals[p.j].idx
				searchStart := time.Now()
				paths, _ := RouteK(g, si, gi, algo, searchK)
				elapsed := time.Since(searchStart).Milliseconds()
				resultCh <- pairResult{
					i:         p.i,
					j:         p.j,
					paths:     paths,
					elapsedMs: elapsed,
				}
			}
		}()
	}

	pairsTried := 0
	go func() {
		for i := 0; i < len(starts) && pairsTried < maxPairs; i++ {
			for j := 0; j < len(goals) && pairsTried < maxPairs; j++ {
				pairsTried++
				pairCh <- pair{i: i, j: j}
				if pairsTried >= maxPairs {
					break
				}
			}
		}
		close(pairCh)
		wg.Wait()
		close(resultCh)
	}()

	for res := range resultCh {
		phases.SearchMs += res.elapsedMs
		if len(res.paths) == 0 {
			continue
		}
		for _, path := range res.paths {
			total := path.dist + starts[res.i].snap + goals[res.j].snap
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
					c := g.NodeCoords[idx]
					coords = append(coords, [2]float64{float64(c[0]), float64(c[1])})
				}
			coords = append(coords, toLatLon)

			estSec := estimateTravelSeconds(path.path, starts[res.i].snap, goals[res.j].snap, useAccelDecel)
			// Soft time penalty: nudge search away from implausibly short/long
			// options when a target is known, without making travel time the
			// primary objective (that remains geometric).
			if opt.TargetTravelSec > 0 && estSec > 0 {
				diff := math.Abs(estSec - opt.TargetTravelSec)
				// Convert seconds to "pseudo-meters" using ~25 m/s (~90 km/h).
				score += diff * 25.0
				if estSec < opt.TargetTravelSec {
					// Extra penalty for being shorter than target; many "too fast"
					// routes are unrealistic compared to timetable.
					score += (opt.TargetTravelSec - estSec) * 15.0
				}
			}

			pr := &PathResult{
				Coords:             coords,
				DistanceMeters:     total,
				EstimatedTravelSec: estSec,
				WayIDs:             path.ways,
				SnapFromMeters:     starts[res.i].snap,
				SnapToMeters:       goals[res.j].snap,
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
	}
	phases.RouteKCalls += pairsTried
	phases.PairsTried = pairsTried
	pairElapsed := time.Since(pairTimingStart).Milliseconds()
	if pairElapsed > phases.SearchMs {
		phases.PairGenerationMs = pairElapsed - phases.SearchMs
	}

	if len(best) == 0 {
		phases.TotalMs = time.Since(t0).Milliseconds()
		return []*PathResult{{GenerationStatus: "no-route", Timings: PathTimings{RouteMs: phases.TotalMs, Phases: &phases}}}, phases, nil
	}
	rankingStart := time.Now()
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
	phases.RankingMs = time.Since(rankingStart).Milliseconds()
	phases.CandidatesFound = len(best)
	phases.TotalMs = time.Since(t0).Milliseconds()
	out := make([]*PathResult, 0, len(best))
	for _, s := range best {
		s.res.Timings = PathTimings{RouteMs: s.res.Timings.RouteMs, Phases: &phases}
		out = append(out, s.res)
	}
	return out, phases, nil
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

func Route(g *RailGraph, start, goal int, algo string) ([]int, float64, []int64, []routeEdgeMeta, RouteStats, bool) {
	return RouteWithConstraints(g, start, goal, algo, nil, nil)
}

func RouteWithConstraints(
	g *RailGraph,
	start,
	goal int,
	algo string,
	bannedEdges map[edgeKey]struct{},
	bannedNodes map[int]struct{},
) ([]int, float64, []int64, []routeEdgeMeta, RouteStats, bool) {
	switch algo {
	case "dijkstra":
		return dijkstraWithConstraints(g, start, goal, bannedEdges, bannedNodes)
	default:
		return astarWithConstraints(g, start, goal, bannedEdges, bannedNodes)
	}
}

func RouteK(g *RailGraph, start, goal int, algo string, k int) ([]routeCandidate, error) {
	cacheKey := routeKCacheKey{start: start, goal: goal, k: k, algo: algo}
	if cached, ok := getRouteKFromCache(cacheKey); ok {
		return cached, nil
	}

	k = maxInt(1, k)
	t0 := time.Now()
	firstNodes, firstDist, firstWays, firstEdges, firstStats, ok := RouteWithConstraints(g, start, goal, algo, nil, nil)
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
		path:    firstEdges,
	}}
	used := map[string]struct{}{best[0].key: {}}
	if k == 1 {
		return best, nil
	}

	// 1) Corridor-span bans from baseline route.
	for _, span := range corridorSpansFromPath(firstNodes, best[0].path, minInt(6, k*3)) {
		if len(best) >= k {
			break
		}
		bannedEdges := make(map[edgeKey]struct{}, len(span.edges)*2)
		for _, e := range span.edges {
			bannedEdges[e] = struct{}{}
			bannedEdges[edgeKey{from: e.to, to: e.from}] = struct{}{}
		}
		rt0 := time.Now()
		altNodes, altDist, altWays, altEdges, altStats, altOk := RouteWithConstraints(g, start, goal, algo, bannedEdges, nil)
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
			path:    altEdges,
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
			altNodes, altDist, altWays, altEdges, altStats, altOk := RouteWithConstraints(g, start, goal, algo, bannedEdges, nil)
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
				path:    altEdges,
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
	putRouteKInCache(cacheKey, best)
	return best, nil
}

func astar(g *RailGraph, start, goal int) ([]int, float64, []int64, RouteStats, bool) {
	path, dist, ways, _, stats, ok := astarWithConstraints(g, start, goal, nil, nil)
	return path, dist, ways, stats, ok
}

func astarWithConstraints(g *RailGraph, start, goal int, bannedEdges map[edgeKey]struct{}, bannedNodes map[int]struct{}) ([]int, float64, []int64, []routeEdgeMeta, RouteStats, bool) {
	return routeWithConstraints(g, start, goal, bannedEdges, bannedNodes, true)
}

func dijkstra(g *RailGraph, start, goal int) ([]int, float64, []int64, RouteStats, bool) {
	path, dist, ways, _, stats, ok := dijkstraWithConstraints(g, start, goal, nil, nil)
	return path, dist, ways, stats, ok
}

func dijkstraWithConstraints(g *RailGraph, start, goal int, bannedEdges map[edgeKey]struct{}, bannedNodes map[int]struct{}) ([]int, float64, []int64, []routeEdgeMeta, RouteStats, bool) {
	return routeWithConstraints(g, start, goal, bannedEdges, bannedNodes, false)
}

func routeWithConstraints(g *RailGraph, start, goal int, bannedEdges map[edgeKey]struct{}, bannedNodes map[int]struct{}, useHeuristic bool) ([]int, float64, []int64, []routeEdgeMeta, RouteStats, bool) {
	if start < 0 || goal < 0 || start >= len(g.NodeCoords) || goal >= len(g.NodeCoords) {
		return nil, 0, nil, nil, RouteStats{}, false
	}

	state := acquireRouteSearchState(len(g.NodeCoords))
	defer releaseRouteSearchState(state)

	state.pq = state.pq[:0]
	heap.Init(&state.pq)

	if _, blocked := bannedNodes[start]; blocked {
		return nil, 0, nil, nil, RouteStats{}, false
	}
	state.nodeStamp[start] = state.epoch
	state.dist[start] = 0
	state.prev[start] = -1
	state.prevEdge[start] = routeEdgeMeta{}
	initialPriority := 0.0
	if useHeuristic {
		initialPriority = heuristicM(g, start, goal)
	}
	heap.Push(&state.pq, &pqItem{Node: start, Priority: initialPriority})

	stats := RouteStats{}
	for state.pq.Len() > 0 {
		it := heap.Pop(&state.pq).(*pqItem)
		u := it.Node
		if _, blocked := bannedNodes[u]; blocked {
			continue
		}
		if state.visStamp[u] == state.epoch {
			continue
		}
		state.visStamp[u] = state.epoch
		stats.Visited++
		if u == goal {
			break
		}

		curDist := state.dist[u]
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
			nd := curDist + float64(e.DistM)
			oldDist := math.Inf(1)
			if state.nodeStamp[e.To] == state.epoch {
				oldDist = state.dist[e.To]
			}
			if nd >= oldDist {
				continue
			}
			state.nodeStamp[e.To] = state.epoch
			state.dist[e.To] = nd
			state.prev[e.To] = u
			state.prevEdge[e.To] = routeEdgeMeta{
				wayID:       e.WayID,
				railType:    e.RailType,
				maxSpeedKPH: e.MaxSpeedKPH,
				distM:       e.DistM,
			}
			priority := nd
			if useHeuristic {
				priority = nd + heuristicM(g, e.To, goal)
			}
			heap.Push(&state.pq, &pqItem{Node: e.To, Priority: priority})
		}
	}

	if state.nodeStamp[goal] != state.epoch {
		return nil, 0, nil, nil, stats, false
	}
	path, pathEdges := reconstructPathAndEdges(state.prev, state.prevEdge, start, goal)
	if len(path) == 0 {
		return nil, 0, nil, nil, stats, false
	}
	ways := reconstructWaysFromEdges(pathEdges)
	return path, state.dist[goal], ways, pathEdges, stats, true
}

func acquireRouteSearchState(nodeCount int) *routeSearchState {
	state := routeSearchStatePool.Get().(*routeSearchState)
	state.epoch++
	if state.epoch == 0 {
		state.epoch = 1
		for i := range state.nodeStamp {
			state.nodeStamp[i] = 0
		}
		for i := range state.visStamp {
			state.visStamp[i] = 0
		}
	}
	if cap(state.dist) < nodeCount {
		state.dist = make([]float64, nodeCount)
		state.prev = make([]int, nodeCount)
		state.prevEdge = make([]routeEdgeMeta, nodeCount)
		state.nodeStamp = make([]uint32, nodeCount)
		state.visStamp = make([]uint32, nodeCount)
	} else {
		state.dist = state.dist[:nodeCount]
		state.prev = state.prev[:nodeCount]
		state.prevEdge = state.prevEdge[:nodeCount]
		state.nodeStamp = state.nodeStamp[:nodeCount]
		state.visStamp = state.visStamp[:nodeCount]
	}
	return state
}

func releaseRouteSearchState(state *routeSearchState) {
	routeSearchStatePool.Put(state)
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

func reconstructPathAndEdges(prev []int, prevEdge []routeEdgeMeta, start, goal int) ([]int, []routeEdgeMeta) {
	nodes := make([]int, 0, 256)
	edges := make([]routeEdgeMeta, 0, 255)
	for cur := goal; ; {
		nodes = append(nodes, cur)
		if cur == start {
			break
		}
		if cur < 0 || cur >= len(prev) {
			return nil, nil
		}
		p := prev[cur]
		if p < 0 || p >= len(prev) {
			return nil, nil
		}
		edges = append(edges, prevEdge[cur])
		cur = p
	}
	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
	// edges are already captured from the reversed direction; reverse to match forward nodes.
	for i, j := 0, len(edges)-1; i < j; i, j = i+1, j-1 {
		edges[i], edges[j] = edges[j], edges[i]
	}
	return nodes, edges
}

func reconstructWaysFromEdges(pathEdges []routeEdgeMeta) []int64 {
	if len(pathEdges) == 0 {
		return nil
	}
	ways := make([]int64, 0, len(pathEdges))
	var last int64
	for _, e := range pathEdges {
		if e.wayID == 0 || e.wayID == last {
			continue
		}
		ways = append(ways, e.wayID)
		last = e.wayID
	}
	return ways
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

func estimateTravelSeconds(pathEdges []routeEdgeMeta, snapFromMeters, snapToMeters float64, useAccelDecel bool) float64 {
	if len(pathEdges) == 0 {
		return 0
	}
	totalSec := 0.0
	startSpeed := 0.0
	endSpeed := 0.0
	prevSpeedKPH := 0.0
	transitionSec := 0.0
	havePrevSpeed := false
	for i, e := range pathEdges {
		speedKPH := ratedSpeedKPH(e.maxSpeedKPH, e.railType)
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
		totalSec += float64(e.distM) / (speedKPH * 1000 / 3600)
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
	bestDist := float32(math.Inf(1))
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

func ratedSpeedKPH(maxSpeedKPH float32, railType uint8) float64 {
	if maxSpeedKPH > 0 {
		return float64(maxSpeedKPH)
	}
	switch railType {
	case railTypeHighSpeed:
		return 250
	case railTypeRail:
		return 120
	case railTypeLightRail:
		return 90
	case railTypeSubway:
		return 80
	case railTypeTram:
		return 50
	case railTypeMonorail:
		return 70
	case railTypeNarrowGauge:
		return 60
	default:
		return 100
	}
}

// wayRatedSpeedKPH remains in server.go where OSM tags are parsed and reduced.

// parseOSMSpeedKPH is implemented in server.go (OSM parsing side).

func heuristicM(g *RailGraph, a, b int) float64 {
	la, lo := g.NodeCoords[a][0], g.NodeCoords[a][1]
	lb, lob := g.NodeCoords[b][0], g.NodeCoords[b][1]
	return haversineMeters(float64(la), float64(lo), float64(lb), float64(lob))
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
	best := float32(math.Inf(1))
	for _, e := range g.Adj[from] {
		if e.To == to && e.DistM < best {
			best = e.DistM
		}
	}
	if math.IsInf(float64(best), 1) {
		return 0, false
	}
	return float64(best), true
}

func pathToWayIDs(g *RailGraph, nodes []int) []int64 {
	if len(nodes) < 2 {
		return nil
	}
	ways := make([]int64, 0, len(nodes))
	var last int64
	for i := 1; i < len(nodes); i++ {
		best := int64(0)
		bestDist := float32(math.Inf(1))
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

func corridorSpansFromPath(nodes []int, pathEdges []routeEdgeMeta, maxSpans int) []corridorSpan {
	if len(nodes) < 2 {
		return nil
	}
	if len(pathEdges) < len(nodes)-1 {
		return nil
	}
	if maxSpans <= 0 {
		maxSpans = 1
	}

	seen := make(map[int64]struct{}, maxSpans)
	spans := make([]corridorSpan, 0, minInt(len(nodes)/8+1, maxSpans))

	for i := 0; i+1 < len(nodes); i++ {
		wayID := pathEdges[i].wayID
		if wayID <= 0 {
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
	bestDist := float32(math.Inf(1))
	for _, e := range g.Adj[from] {
		if e.To != to {
			continue
		}
		if e.DistM < bestDist {
			bestDist = e.DistM
			best = e.WayID
		}
	}
	if best == 0 || math.IsInf(float64(bestDist), 1) {
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
		vx, vy := approxPlanarDeltaMeters(float64(a[0]), float64(a[1]), float64(b[0]), float64(b[1]))
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
