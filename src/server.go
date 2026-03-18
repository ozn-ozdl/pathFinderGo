package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
	"github.com/paulmach/osm/osmxml"
)

type RailwayStation struct {
	ID   osm.NodeID        `json:"id"`
	Name string            `json:"name"`
	Type string            `json:"type"`
	Lat  float64           `json:"lat"`
	Lon  float64           `json:"lon"`
	Tags map[string]string `json:"tags,omitempty"`
}

type RailwayLine struct {
	ID      osm.WayID    `json:"id"`
	Name    string       `json:"name"`
	Type    string       `json:"type"`
	NodeIDs []osm.NodeID `json:"node_ids"`
	// Coords is the ordered list of coordinates along the way, derived from NodeIDs.
	Coords []struct {
		Lat float64 `json:"lat"`
		Lon float64 `json:"lon"`
	} `json:"coords"`
	Tags osm.Tags `json:"tags,omitempty"`
}

type RailwayData struct {
	StationsByID map[osm.NodeID]RailwayStation `json:"stations_by_id"`
	LinesByID    map[osm.WayID]RailwayLine     `json:"lines_by_id"`
}

type Server struct {
	data       RailwayData
	graph      *RailGraph
	ifoptIndex map[string][]osm.NodeID
	mux        *http.ServeMux
}

const cacheSchemaVersion = "v3-filtered-rail-types"
var allowedRailwayWayTypes = []string{"rail", "narrow_gauge", "monorail"}
var allowedRailwayWayTypeSet = map[string]struct{}{
	"rail":         {},
	"narrow_gauge": {},
	"monorail":     {},
}

func main() {
	loadEnvFile(".env")

	defaultAddr := getEnv("SERVER_ADDR", ":8080")
	defaultOSM := getOSMPath()

	addr := flag.String("addr", defaultAddr, "HTTP listen address (or set SERVER_ADDR)")
	osmPath := flag.String("osm", defaultOSM, "Path to OSM extract (.osm or .pbf) (or set OSM_FILE)")
	flag.Parse()

	log.Printf("active railway way types: %s", strings.Join(allowedRailwayWayTypes, ", "))
	log.Printf("loading OSM file from %s", *osmPath)
	data, graph, err := loadRailwayDataAndGraph(*osmPath)
	if err != nil {
		log.Fatalf("failed to load OSM file: %v", err)
	}

	s := &Server{
		data:       data,
		graph:      graph,
		ifoptIndex: buildIFOPTIndex(data),
		mux:        http.NewServeMux(),
	}
	s.registerRoutes()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("server listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("server error: %v", err)
	}
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// v1 API (preferred)
	s.mux.HandleFunc("/api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("/api/v1/stations", s.handleStations)
	s.mux.HandleFunc("/api/v1/lines", s.handleLines)
	s.mux.HandleFunc("/api/v1/route", s.handleRouteV1)
	s.mux.HandleFunc("/api/v1/trip/paths", s.handleTripPathsV1)

	// Legacy endpoints (kept for compatibility with older clients/UI)
	s.mux.HandleFunc("/railway/stations", s.handleStations)
	s.mux.HandleFunc("/railway/lines", s.handleLines)
	s.mux.HandleFunc("/railway/route", s.handleRoute)
	s.mux.HandleFunc("/railway/trip/paths", s.handleTripPaths)
}

type StopRef struct {
	// Prefer one of: ifopt OR station_id OR coordinates (lat/lon).
	IFOPT string `json:"ifopt"`

	StationID json.RawMessage `json:"station_id"`
	ID        json.RawMessage `json:"id"` // legacy-ish alias

	Lat *float64 `json:"lat"`
	Lon *float64 `json:"lon"`
}

type RouteRequest struct {
	From StopRef `json:"from"`
	To   StopRef `json:"to"`
	K    int     `json:"k"`
	Algo string  `json:"algo"`

	UseAccelDecel *bool `json:"use_accel_decel"`
	// Optional corridor similarity threshold for filtering near-duplicate alternatives.
	// Range: [0,1]. Higher values allow more similar alternatives.
	CorridorSimilarityThreshold *float64 `json:"corridor_similarity_threshold"`
}

type RouteResponse struct {
	From    ResolvedStop  `json:"from"`
	To      ResolvedStop  `json:"to"`
	TopK    []*PathResult `json:"top_k"`
	Timings struct {
		DecodeMs  int64 `json:"decode_ms"`
		ResolveMs int64 `json:"resolve_ms"`
		RouteMs   int64 `json:"route_ms"`
		TotalMs   int64 `json:"total_ms"`
	} `json:"timings"`
}

type TripPathsRequest struct {
	IFOPTIDs []string       `json:"ifopt_ids"`
	IFOPTIds []string       `json:"ifoptIds"`
	Stops    []TripStopSpec `json:"stops"`
	K        int            `json:"k"`
	Algo     string         `json:"algo"`
	// Optional corridor similarity threshold for filtering near-duplicate alternatives.
	// Range: [0,1]. Higher values allow more similar alternatives.
	CorridorSimilarityThreshold *float64 `json:"corridor_similarity_threshold"`
	// Optional: include accel/decel penalties in estimated travel time (default true).
	UseAccelDecelSnake *bool `json:"use_accel_decel"`
	UseAccelDecelCamel *bool `json:"useAccelDecel"`
	// Optional scheduled timing (seconds since midnight) for whole journey.
	DepartureSec *int `json:"departure_sec"`
	ArrivalSec   *int `json:"arrival_sec"`
	// Optional explicit target travel time (seconds) used for ranking.
	// For single-segment trips, this is used as the segment target if no legs match.
	TargetTravelSec *float64 `json:"target_travel_sec"`
	// Optional journey legs used to rank each segment by real-world scheduled time.
	Legs []TripTimingLeg `json:"legs"`
}

type TripTimingLeg struct {
	Type         string `json:"type"`
	FromStopID   string `json:"from_stop_id"`
	ToStopID     string `json:"to_stop_id"`
	DepartureSec *int   `json:"departure_sec"`
	ArrivalSec   *int   `json:"arrival_sec"`
}

type TripPathsV1Request struct {
	Stops []StopRef       `json:"stops"`
	K     int             `json:"k"`
	Algo  string          `json:"algo"`
	Legs  []TripTimingLeg `json:"legs"`

	UseAccelDecel *bool `json:"use_accel_decel"`
	// Optional corridor similarity threshold for filtering near-duplicate alternatives.
	// Range: [0,1]. Higher values allow more similar alternatives.
	CorridorSimilarityThreshold *float64 `json:"corridor_similarity_threshold"`

	DepartureSec    *int     `json:"departure_sec"`
	ArrivalSec      *int     `json:"arrival_sec"`
	TargetTravelSec *float64 `json:"target_travel_sec"`
}

func (r TripPathsRequest) UseAccelDecel() bool {
	if r.UseAccelDecelSnake != nil {
		return *r.UseAccelDecelSnake
	}
	if r.UseAccelDecelCamel != nil {
		return *r.UseAccelDecelCamel
	}
	return true
}

type TripStopSpec struct {
	IFOPT     *string         `json:"ifopt"`
	IFOPTID   *string         `json:"ifopt_id"`
	ID        json.RawMessage `json:"id"`
	StopID    json.RawMessage `json:"stop_id"`
	StationID json.RawMessage `json:"station_id"`
	Lat       *float64        `json:"lat"`
	Lon       *float64        `json:"lon"`
	Long      *float64        `json:"long"`
	Longitude *float64        `json:"longitude"`
	Latitude  *float64        `json:"latitude"`
}

type ResolvedStop struct {
	InputIFOPT string          `json:"input_ifopt"`
	Prefix     string          `json:"prefix"`
	InputID    string          `json:"input_id,omitempty"`
	InputLat   *float64        `json:"input_lat,omitempty"`
	InputLon   *float64        `json:"input_lon,omitempty"`
	Matched    bool            `json:"matched"`
	Station    *RailwayStation `json:"station,omitempty"`
}

type SegmentPaths struct {
	From    ResolvedStop   `json:"from"`
	To      ResolvedStop   `json:"to"`
	TopK    []*PathResult  `json:"top_k"`
	Timings SegmentTimings `json:"timings"`
}

type SegmentTimings struct {
	ResolveMs int64 `json:"resolve_ms"`
	RouteMs   int64 `json:"route_ms"`
}

type TripPathsResponse struct {
	K         int            `json:"k"`
	Algo      string         `json:"algo"`
	Stops     []ResolvedStop `json:"stops"`
	Segments  []SegmentPaths `json:"segments"`
	Unmatched []ResolvedStop `json:"unmatched"`
	Timings   TripTimings    `json:"timings"`
}

type TripTimings struct {
	TotalMs   int64 `json:"total_ms"`
	DecodeMs  int64 `json:"decode_ms"`
	ResolveMs int64 `json:"resolve_ms"`
	RouteMs   int64 `json:"route_ms"`
}

func (s *Server) handleTripPathsV1(w http.ResponseWriter, r *http.Request) {
	reqID := fmt.Sprintf("trip-paths-v1-%d", time.Now().UnixNano())
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Max-Age", "86400")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.graph == nil {
		http.Error(w, "graph not ready", http.StatusServiceUnavailable)
		return
	}

	t0 := time.Now()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	tParse0 := time.Now()
	var reqV1 TripPathsV1Request
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&reqV1); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	decodeMs := time.Since(tParse0).Milliseconds()

	legacy := TripPathsRequest{
		K:                           reqV1.K,
		Algo:                        reqV1.Algo,
		CorridorSimilarityThreshold: reqV1.CorridorSimilarityThreshold,
		UseAccelDecelSnake:          reqV1.UseAccelDecel,
		DepartureSec:                reqV1.DepartureSec,
		ArrivalSec:                  reqV1.ArrivalSec,
		TargetTravelSec:             reqV1.TargetTravelSec,
		Legs:                        reqV1.Legs,
	}
	for _, st := range reqV1.Stops {
		legacy.Stops = append(legacy.Stops, stopRefToTripStopSpec(st))
	}

	// Reuse the legacy handler logic by calling the core implementation.
	resp, status, httpErr := s.computeTripPaths(reqID, legacy, decodeMs)
	if httpErr != nil {
		http.Error(w, httpErr.Error(), status)
		return
	}
	resp.Timings.TotalMs = time.Since(t0).Milliseconds()
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleTripPaths(w http.ResponseWriter, r *http.Request) {
	// CORS (needed for browser clients; also handles preflight OPTIONS).
	reqID := fmt.Sprintf("trip-paths-%d", time.Now().UnixNano())
	log.Printf("[%s] incoming %s %s from %s", reqID, r.Method, r.URL.Path, r.RemoteAddr)
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Max-Age", "86400")
	if r.Method == http.MethodOptions {
		log.Printf("[%s] preflight OPTIONS handled", reqID)
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		log.Printf("[%s] rejected method %s", reqID, r.Method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.graph == nil {
		log.Printf("[%s] graph not ready", reqID)
		http.Error(w, "graph not ready", http.StatusServiceUnavailable)
		return
	}

	t0 := time.Now()
	tRead0 := time.Now()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[%s] failed reading body: %v", reqID, err)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	readMs := time.Since(tRead0).Milliseconds()
	log.Printf("[%s] body_bytes=%d", reqID, len(body))
	var req TripPathsRequest
	parseMode := "TripPathsRequest"
	tParse0 := time.Now()
	{
		dec := json.NewDecoder(bytes.NewReader(body))
		if err := dec.Decode(&req); err != nil {
			log.Printf("[%s] decode as TripPathsRequest failed: %v", reqID, err)
			var ids []string
			var stops []TripStopSpec
			dec = json.NewDecoder(bytes.NewReader(body))
			if err2 := dec.Decode(&ids); err2 == nil {
				parseMode = "string_array"
				for _, id := range ids {
					id = strings.TrimSpace(id)
					if id == "" {
						continue
					}
					stop := TripStopSpec{}
					idCopy := id
					stop.IFOPT = &idCopy
					req.Stops = append(req.Stops, stop)
				}
			} else if err3 := dec.Decode(&stops); err3 == nil && len(stops) > 0 {
				parseMode = "stops_array"
				req.Stops = stops
			} else {
				log.Printf("[%s] failed parsing request body as all supported formats", reqID)
				http.Error(w, "invalid json body", http.StatusBadRequest)
				return
			}
		}
		log.Printf("[%s] parse_mode=%s parse_ms=%d", reqID, parseMode, time.Since(tParse0).Milliseconds())
	}
	decodeMs := time.Since(tParse0).Milliseconds()
	log.Printf("[%s] request parsed with %d stops (ifopt_ids=%d, ifoptIds=%d) read_ms=%d", reqID, len(req.Stops), len(req.IFOPTIDs), len(req.IFOPTIds), readMs)

	// Backward compatibility: object with ifoptIds key and object with stops are both supported.
	if len(req.IFOPTIDs) == 0 && len(req.IFOPTIds) > 0 {
		log.Printf("[%s] backfilling req.IFOPTIDs from req.IFOPTIds", reqID)
		req.IFOPTIDs = req.IFOPTIds
	}
	if len(req.Stops) == 0 && len(req.IFOPTIDs) > 0 {
		log.Printf("[%s] expanding %d ifopt_ids into stops", reqID, len(req.IFOPTIDs))
		for _, raw := range req.IFOPTIDs {
			idCopy := strings.TrimSpace(raw)
			stop := TripStopSpec{}
			stop.IFOPT = &idCopy
			req.Stops = append(req.Stops, stop)
		}
	}
	if len(req.Stops) < 2 {
		log.Printf("[%s] invalid request: need at least 2 stops (got %d)", reqID, len(req.Stops))
		http.Error(w, "need at least 2 stops (ifopt_ids, stops, or raw string array)", http.StatusBadRequest)
		return
	}

	resp, status, httpErr := s.computeTripPaths(reqID, req, decodeMs)
	if httpErr != nil {
		http.Error(w, httpErr.Error(), status)
		return
	}
	resp.Timings.TotalMs = time.Since(t0).Milliseconds()
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) computeTripPaths(reqID string, req TripPathsRequest, decodeMs int64) (TripPathsResponse, int, error) {
	k := clampInt(req.K, 1, 10)
	algo := strings.ToLower(strings.TrimSpace(req.Algo))
	if algo == "" {
		algo = "astar"
	}
	useAccelDecel := req.UseAccelDecel()
	corridorThreshold := 0.82
	if req.CorridorSimilarityThreshold != nil {
		corridorThreshold = *req.CorridorSimilarityThreshold
	}
	if corridorThreshold < 0 {
		corridorThreshold = 0
	}
	if corridorThreshold > 1 {
		corridorThreshold = 1
	}
	log.Printf("[%s] request config k=%d algo=%s use_accel_decel=%t corridor_similarity_threshold=%.3f", reqID, k, algo, useAccelDecel, corridorThreshold)

	tResolve0 := time.Now()
	stops := make([]ResolvedStop, 0, len(req.Stops))
	unmatched := make([]ResolvedStop, 0)

	for _, raw := range req.Stops {
		tStopResolve := time.Now()
		rs, ok := s.resolveTripStop(raw)
		if !ok {
			unmatched = append(unmatched, rs)
		}
		log.Printf(
			"[%s] stop resolved matched=%t input=%s result=%s elapsed_ms=%d",
			reqID,
			ok,
			describeTripStopRaw(raw),
			describeResolvedStop(rs),
			time.Since(tStopResolve).Milliseconds(),
		)
		stops = append(stops, rs)
	}
	resolveMs := time.Since(tResolve0).Milliseconds()
	log.Printf("[%s] resolved stops=%d unmatched=%d resolve_ms=%d", reqID, len(stops), len(unmatched), resolveMs)

	var totalRouteMs int64
	segments := make([]SegmentPaths, 0, len(stops)-1)
	for i := 0; i+1 < len(stops); i++ {
		segResolve0 := time.Now()
		a := stops[i]
		b := stops[i+1]
		seg := SegmentPaths{From: a, To: b, TopK: nil, Timings: SegmentTimings{}}
		seg.Timings.ResolveMs = time.Since(segResolve0).Milliseconds()
		log.Printf("[%s] segment %d resolve_ms=%d", reqID, i, seg.Timings.ResolveMs)
		aCoord, aHasCoord := stopCoords(a)
		bCoord, bHasCoord := stopCoords(b)
		if a.Matched && b.Matched && aHasCoord && bHasCoord {
			segRoute0 := time.Now()
			start := aCoord
			goal := bCoord
			log.Printf("[%s] segment %d routing start latlon=(%.6f,%.6f)->(%.6f,%.6f)", reqID, i, start[0], start[1], goal[0], goal[1])
			topk, routeErr := RouteStationsTopK(s.graph, start, goal, TopKOptions{
				K:                           k,
				SnapK:                       10,
				MaxPairs:                    220,
				Algorithm:                   algo,
				UseAccelDecel:               useAccelDecel,
				CorridorSimilarityThreshold: corridorThreshold,
			})
			if routeErr != nil {
				log.Printf("[%s] segment %d route error: %v", reqID, i, routeErr)
			}
			seg.TopK = topk
			targetSec, hasTarget := segmentTargetTravelSeconds(req, a, b, len(stops)-1)
			if hasTarget {
				rankTopKByTargetTravelTime(seg.TopK, targetSec)
			}
			annotateTopKRanking(seg.TopK, targetSec, hasTarget)
			seg.Timings.RouteMs = time.Since(segRoute0).Milliseconds()
			totalRouteMs += seg.Timings.RouteMs
			best := "none"
			if len(topk) > 0 {
				best = topk[0].GenerationStatus
			}
			log.Printf(
				"[%s] segment %d route_ms=%d topk=%d best_status=%s",
				reqID,
				i,
				seg.Timings.RouteMs,
				len(topk),
				best,
			)
		} else {
			log.Printf(
				"[%s] segment %d skipped route (from matched=%t coord=%t, to matched=%t coord=%t)",
				reqID,
				i,
				a.Matched,
				aHasCoord,
				b.Matched,
				bHasCoord,
			)
		}
		segments = append(segments, seg)
	}

	resp := TripPathsResponse{
		K:         k,
		Algo:      algo,
		Stops:     stops,
		Segments:  segments,
		Unmatched: unmatched,
		Timings: TripTimings{
			TotalMs:   0,
			DecodeMs:  decodeMs,
			ResolveMs: resolveMs,
			RouteMs:   totalRouteMs,
		},
	}
	return resp, http.StatusOK, nil
}

func describeTripStopRaw(raw TripStopSpec) string {
	parts := make([]string, 0, 8)
	if raw.IFOPT != nil {
		parts = append(parts, fmt.Sprintf("ifopt=%q", strings.TrimSpace(*raw.IFOPT)))
	}
	if raw.IFOPTID != nil {
		parts = append(parts, fmt.Sprintf("ifopt_id=%q", strings.TrimSpace(*raw.IFOPTID)))
	}
	if raw.ID != nil {
		parts = append(parts, fmt.Sprintf("id=%q", strings.TrimSpace(string(raw.ID))))
	}
	if raw.StationID != nil {
		parts = append(parts, fmt.Sprintf("station_id=%q", strings.TrimSpace(string(raw.StationID))))
	}
	if raw.StopID != nil {
		parts = append(parts, fmt.Sprintf("stop_id=%q", strings.TrimSpace(string(raw.StopID))))
	}
	if raw.Lat != nil && raw.Lon != nil {
		parts = append(parts, fmt.Sprintf("lat=%.6f lon=%.6f", *raw.Lat, *raw.Lon))
	}
	if len(parts) == 0 {
		return "empty"
	}
	return strings.Join(parts, ",")
}

func describeResolvedStop(stop ResolvedStop) string {
	if stop.Station != nil {
		return fmt.Sprintf("station id=%d name=%q matched=%t prefix=%q", stop.Station.ID, stop.Station.Name, stop.Matched, stop.Prefix)
	}
	if stop.InputIFOPT != "" {
		lat := "?"
		lon := "?"
		if stop.InputLat != nil {
			lat = fmt.Sprintf("%.6f", *stop.InputLat)
		}
		if stop.InputLon != nil {
			lon = fmt.Sprintf("%.6f", *stop.InputLon)
		}
		return fmt.Sprintf("fallback_input ifopt=%q prefix=%q matched=%t lat=%s lon=%s", stop.InputIFOPT, stop.Prefix, stop.Matched, lat, lon)
	}
	return fmt.Sprintf("unmatched prefix=%q matched=%t", stop.Prefix, stop.Matched)
}

func (s *Server) resolveTripStop(raw TripStopSpec) (ResolvedStop, bool) {
	rs := ResolvedStop{}
	resolveByCoordinates := func() (ResolvedStop, bool) {
		lat, latOK := firstFloat64(raw.Lat, raw.Latitude)
		lon, lonOK := firstFloat64(raw.Lon, raw.Long, raw.Longitude)
		if latOK && lonOK && isValidLatLon(lat, lon) {
			rs.InputLat = &lat
			rs.InputLon = &lon
			rs.Matched = true
			if rs.Prefix == "" {
				rs.Prefix = "coordinates"
			}
			return rs, true
		}
		return rs, false
	}
	if raw.IFOPT != nil && strings.TrimSpace(*raw.IFOPT) != "" {
		input := strings.TrimSpace(*raw.IFOPT)
		rs.InputIFOPT = input
		rs.Prefix = ifoptPrefix(input)
		if rs.Prefix != "" {
			cands := s.ifoptIndex[rs.Prefix]
			if len(cands) > 0 {
				station, ok := s.data.StationsByID[cands[0]]
				if ok {
					rs.Matched = true
					rs.Station = &station
					return rs, true
				}
			}
		}
		return resolveByCoordinates()
	}

	if raw.IFOPTID != nil && strings.TrimSpace(*raw.IFOPTID) != "" {
		input := strings.TrimSpace(*raw.IFOPTID)
		rs.InputIFOPT = input
		rs.InputID = input
		rs.Prefix = "ifopt_id"
		if resolved, ok := s.resolveTripStopByIFOPT(&rs, input); ok {
			return resolved, true
		}
		return resolveByCoordinates()
	}

	if id, found := parseRawID(raw.ID); found {
		rs.Prefix = "station_id"
		rs.InputID = id
		rs.InputIFOPT = id
		if resolved, ok := s.resolveTripStopByStationID(&rs, id); ok {
			return resolved, true
		}
		return resolveByCoordinates()
	}
	if id, found := parseRawID(raw.StationID); found {
		rs.Prefix = "station_id"
		rs.InputID = id
		rs.InputIFOPT = id
		if resolved, ok := s.resolveTripStopByStationID(&rs, id); ok {
			return resolved, true
		}
		return resolveByCoordinates()
	}
	if id, found := parseRawID(raw.StopID); found {
		rs.Prefix = "station_id"
		rs.InputID = id
		rs.InputIFOPT = id
		if resolved, ok := s.resolveTripStopByStationID(&rs, id); ok {
			return resolved, true
		}
		return resolveByCoordinates()
	}

	if resolved, ok := resolveByCoordinates(); ok {
		return resolved, true
	}

	return rs, false
}

func (s *Server) resolveTripStopByIFOPT(rs *ResolvedStop, ifopt string) (ResolvedStop, bool) {
	rs.Prefix = ifoptPrefix(ifopt)
	if rs.Prefix == "" {
		return *rs, false
	}
	cands := s.ifoptIndex[rs.Prefix]
	if len(cands) == 0 {
		return *rs, false
	}
	st, ok := s.data.StationsByID[cands[0]]
	if !ok {
		return *rs, false
	}
	rs.Matched = true
	rs.Station = &st
	return *rs, true
}

func (s *Server) resolveTripStopByStationID(rs *ResolvedStop, id string) (ResolvedStop, bool) {
	rs.InputID = id
	if rs.InputIFOPT == "" {
		rs.InputIFOPT = id
	}
	stationID, err := parseInt64(id)
	if err != nil {
		return *rs, false
	}
	st, ok := s.data.StationsByID[osm.NodeID(stationID)]
	if !ok {
		return *rs, false
	}
	rs.Station = &st
	rs.Matched = true
	return *rs, true
}

func parseRawID(raw json.RawMessage) (string, bool) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return "", false
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil && strings.TrimSpace(asString) != "" {
		return strings.TrimSpace(asString), true
	}
	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return strings.TrimSpace(asNumber.String()), true
	}
	return "", false
}

func firstFloat64(opts ...*float64) (float64, bool) {
	for _, v := range opts {
		if v != nil {
			return *v, true
		}
	}
	return 0, false
}

func isValidLatLon(lat, lon float64) bool {
	return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}

func stopCoords(stop ResolvedStop) ([2]float64, bool) {
	if stop.Station != nil {
		return [2]float64{stop.Station.Lat, stop.Station.Lon}, true
	}
	if stop.InputLat != nil && stop.InputLon != nil {
		return [2]float64{*stop.InputLat, *stop.InputLon}, true
	}
	return [2]float64{}, false
}

func rankTopKByTargetTravelTime(topk []*PathResult, targetSec float64) {
	if len(topk) <= 1 || targetSec <= 0 {
		return
	}
	sort.SliceStable(topk, func(i, j int) bool {
		ti := topk[i].EstimatedTravelSec
		tj := topk[j].EstimatedTravelSec

		// Prefer options that do not exceed scheduled time.
		underI := ti <= targetSec
		underJ := tj <= targetSec
		if underI != underJ {
			return underI
		}

		if underI && underJ {
			// "Closest without going over": largest estimate still <= target.
			if ti != tj {
				return ti > tj
			}
		} else {
			// If all are over target, pick smallest overshoot.
			oi := ti - targetSec
			oj := tj - targetSec
			if oi != oj {
				return oi < oj
			}
		}

		// Tie-breaker keeps deterministic ordering.
		return topk[i].DistanceMeters < topk[j].DistanceMeters
	})
}

func segmentTargetTravelSeconds(req TripPathsRequest, from, to ResolvedStop, segmentCount int) (float64, bool) {
	// 1) Prefer explicit leg timing for the exact segment (from_stop_id -> to_stop_id).
	fromKey := canonicalStopKey(from)
	toKey := canonicalStopKey(to)
	if fromKey != "" && toKey != "" {
		for _, leg := range req.Legs {
			if leg.DurationSeconds() <= 0 {
				continue
			}
			if strings.ToLower(strings.TrimSpace(leg.Type)) == "walk" {
				continue
			}
			if canonicalIFOPT(leg.FromStopID) == fromKey && canonicalIFOPT(leg.ToStopID) == toKey {
				return leg.DurationSeconds(), true
			}
		}
	}

	// 2) Fallback: if an explicit target is provided and this is a single segment, use it.
	if segmentCount == 1 && req.TargetTravelSec != nil && *req.TargetTravelSec > 0 {
		return *req.TargetTravelSec, true
	}

	// 3) Fallback: if journey timing exists and this is a single segment, use total duration.
	if segmentCount == 1 && req.DepartureSec != nil && req.ArrivalSec != nil {
		delta := *req.ArrivalSec - *req.DepartureSec
		if delta > 0 {
			return float64(delta), true
		}
	}

	return 0, false
}

func canonicalStopKey(stop ResolvedStop) string {
	if stop.InputIFOPT != "" {
		return canonicalIFOPT(stop.InputIFOPT)
	}
	if stop.InputID != "" {
		return canonicalIFOPT(stop.InputID)
	}
	if stop.Station != nil && stop.Station.Tags != nil {
		if v := strings.TrimSpace(stop.Station.Tags["ref:IFOPT"]); v != "" {
			return canonicalIFOPT(v)
		}
	}
	return ""
}

func canonicalIFOPT(v string) string {
	return strings.ToLower(strings.TrimSpace(v))
}

func (l TripTimingLeg) DurationSeconds() float64 {
	if l.DepartureSec == nil || l.ArrivalSec == nil {
		return 0
	}
	delta := *l.ArrivalSec - *l.DepartureSec
	if delta <= 0 {
		return 0
	}
	return float64(delta)
}

func (s *Server) handleStations(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")

	stations := make([]RailwayStation, 0, len(s.data.StationsByID))
	for _, st := range s.data.StationsByID {
		stations = append(stations, st)
	}
	writeJSON(w, http.StatusOK, stations)
}

func (s *Server) handleLines(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")

	lines := make([]RailwayLine, 0, len(s.data.LinesByID))
	for _, ln := range s.data.LinesByID {
		lines = append(lines, ln)
	}
	writeJSON(w, http.StatusOK, lines)
}

type PathResult struct {
	Rank               int          `json:"rank,omitempty"`
	Ranking            *PathRanking `json:"ranking,omitempty"`
	Coords             [][2]float64 `json:"coords"`
	DistanceMeters     float64      `json:"distance_meters"`
	EstimatedTravelSec float64      `json:"estimated_travel_sec"`
	WayIDs             []int64      `json:"way_ids"`
	SnapFromMeters     float64      `json:"snap_from_meters"`
	SnapToMeters       float64      `json:"snap_to_meters"`
	VisitedNodes       int          `json:"visited_nodes"`
	RelaxedEdges       int          `json:"relaxed_edges"`
	Algorithm          string       `json:"algorithm"`
	GenerationStatus   string       `json:"generation_status"`
	Timings            PathTimings  `json:"timings"`
}

type PathRanking struct {
	Mode            string  `json:"mode"`
	TargetTravelSec float64 `json:"target_travel_sec"`
	DeltaSec        float64 `json:"delta_sec"`
	UnderTarget     bool    `json:"under_target"`
}

type PathTimings struct {
	RouteMs int64 `json:"route_ms"`
}

func annotateTopKRanking(topk []*PathResult, targetSec float64, hasTarget bool) {
	for i := range topk {
		if topk[i] == nil {
			continue
		}
		topk[i].Rank = i + 1
		if !hasTarget || targetSec <= 0 {
			topk[i].Ranking = nil
			continue
		}
		est := topk[i].EstimatedTravelSec
		topk[i].Ranking = &PathRanking{
			Mode:            "closest_without_going_over",
			TargetTravelSec: targetSec,
			DeltaSec:        est - targetSec,
			UnderTarget:     est <= targetSec,
		}
	}
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if s.graph == nil {
		http.Error(w, "graph not ready", http.StatusServiceUnavailable)
		return
	}

	q := r.URL.Query()
	fromIDStr := strings.TrimSpace(q.Get("fromStationID"))
	toIDStr := strings.TrimSpace(q.Get("toStationID"))
	var start, goal [2]float64
	if fromIDStr != "" && toIDStr != "" {
		fromID, err := parseInt64(fromIDStr)
		if err != nil {
			http.Error(w, "invalid fromStationID", http.StatusBadRequest)
			return
		}
		toID, err := parseInt64(toIDStr)
		if err != nil {
			http.Error(w, "invalid toStationID", http.StatusBadRequest)
			return
		}

		fromSt, ok := s.data.StationsByID[osm.NodeID(fromID)]
		if !ok {
			http.Error(w, "fromStationID not found", http.StatusNotFound)
			return
		}
		toSt, ok := s.data.StationsByID[osm.NodeID(toID)]
		if !ok {
			http.Error(w, "toStationID not found", http.StatusNotFound)
			return
		}
		start = [2]float64{fromSt.Lat, fromSt.Lon}
		goal = [2]float64{toSt.Lat, toSt.Lon}
	} else {
		// Convenience mode: allow routing by coordinates as well.
		// Example: /railway/route?fromLat=48.85&fromLon=2.35&toLat=48.88&toLon=2.29
		fromLat := strings.TrimSpace(q.Get("fromLat"))
		fromLon := strings.TrimSpace(q.Get("fromLon"))
		toLat := strings.TrimSpace(q.Get("toLat"))
		toLon := strings.TrimSpace(q.Get("toLon"))
		if fromLat == "" || fromLon == "" || toLat == "" || toLon == "" {
			http.Error(w, "missing fromStationID/toStationID or fromLat/fromLon/toLat/toLon", http.StatusBadRequest)
			return
		}
		la1, ok := parseFloat64(fromLat)
		if !ok {
			http.Error(w, "invalid fromLat", http.StatusBadRequest)
			return
		}
		lo1, ok := parseFloat64(fromLon)
		if !ok {
			http.Error(w, "invalid fromLon", http.StatusBadRequest)
			return
		}
		la2, ok := parseFloat64(toLat)
		if !ok {
			http.Error(w, "invalid toLat", http.StatusBadRequest)
			return
		}
		lo2, ok := parseFloat64(toLon)
		if !ok {
			http.Error(w, "invalid toLon", http.StatusBadRequest)
			return
		}
		if !isValidLatLon(la1, lo1) || !isValidLatLon(la2, lo2) {
			http.Error(w, "invalid lat/lon range", http.StatusBadRequest)
			return
		}
		start = [2]float64{la1, lo1}
		goal = [2]float64{la2, lo2}
	}

	k := clampInt(parseIntDefault(q.Get("k"), 6), 1, 20)
	algo := strings.ToLower(strings.TrimSpace(q.Get("algo")))
	if algo == "" {
		algo = "astar"
	}
	useAccelDecel := parseBoolDefault(q.Get("use_accel_decel"), true)

	res, httpErr := RouteStations(s.graph, start, goal, RouteOptions{
		K:             k,
		Algorithm:     algo,
		UseAccelDecel: useAccelDecel,
	})
	if httpErr != nil {
		http.Error(w, httpErr.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleRouteV1(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Max-Age", "86400")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.graph == nil {
		http.Error(w, "graph not ready", http.StatusServiceUnavailable)
		return
	}

	t0 := time.Now()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	tParse0 := time.Now()
	var req RouteRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	decodeMs := time.Since(tParse0).Milliseconds()

	tResolve0 := time.Now()
	from, fromOK := s.resolveStopRef(req.From)
	to, toOK := s.resolveStopRef(req.To)
	resolveMs := time.Since(tResolve0).Milliseconds()
	if !fromOK || !toOK {
		http.Error(w, "could not resolve from/to (provide ifopt, station_id, or lat/lon)", http.StatusBadRequest)
		return
	}
	fromCoord, okA := stopCoords(from)
	toCoord, okB := stopCoords(to)
	if !okA || !okB {
		http.Error(w, "could not determine coordinates for from/to", http.StatusBadRequest)
		return
	}

	k := clampInt(req.K, 1, 10)
	algo := strings.ToLower(strings.TrimSpace(req.Algo))
	if algo == "" {
		algo = "astar"
	}
	useAccelDecel := true
	if req.UseAccelDecel != nil {
		useAccelDecel = *req.UseAccelDecel
	}
	corridorThreshold := 0.82
	if req.CorridorSimilarityThreshold != nil {
		corridorThreshold = *req.CorridorSimilarityThreshold
	}
	if corridorThreshold < 0 {
		corridorThreshold = 0
	}
	if corridorThreshold > 1 {
		corridorThreshold = 1
	}

	tRoute0 := time.Now()
	topk, routeErr := RouteStationsTopK(s.graph, fromCoord, toCoord, TopKOptions{
		K:                           k,
		SnapK:                       10,
		MaxPairs:                    220,
		Algorithm:                   algo,
		UseAccelDecel:               useAccelDecel,
		CorridorSimilarityThreshold: corridorThreshold,
	})
	routeMs := time.Since(tRoute0).Milliseconds()
	if routeErr != nil {
		http.Error(w, routeErr.Error(), http.StatusBadRequest)
		return
	}

	var resp RouteResponse
	resp.From = from
	resp.To = to
	resp.TopK = topk
	resp.Timings.DecodeMs = decodeMs
	resp.Timings.ResolveMs = resolveMs
	resp.Timings.RouteMs = routeMs
	resp.Timings.TotalMs = time.Since(t0).Milliseconds()
	writeJSON(w, http.StatusOK, resp)
}

func loadRailwayDataAndGraph(path string) (RailwayData, *RailGraph, error) {
	sig, err := fileSignature(path)
	if err != nil {
		return RailwayData{}, nil, err
	}
	cacheDir := filepath.Join(filepath.Dir(path), ".cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return RailwayData{}, nil, err
	}
	dataCachePath := filepath.Join(cacheDir, fmt.Sprintf("railwaydata-%s.gob", sig))
	graphCachePath := filepath.Join(cacheDir, fmt.Sprintf("railgraph-%s.gob", sig))

	cachedData, dataFromCache, err := loadRailwayDataFromCache(dataCachePath)
	dataFromCacheFlag := dataFromCache
	data := RailwayData{}
	if err == nil && len(cachedData.LinesByID) > 0 {
		log.Printf("railway data cache hit %s", dataCachePath)
		data = cachedData
		dataFromCacheFlag = true
		graph, graphErr := loadGraphGob(graphCachePath)
		if graphErr == nil {
			log.Printf("graph cache hit %s", graphCachePath)
			return data, graph, nil
		}
		log.Printf("railway data cache hit but graph cache missed/failed (%v), rebuilding graph from cached railway data", graphErr)
		nodesByID := rebuildNodesFromCachedData(data)
		graph, buildErr := LoadOrBuildGraph(path, nodesByID, data.LinesByID)
		if buildErr == nil && graph != nil && len(graph.NodeIDs) > 0 {
			return data, graph, nil
		}
		log.Printf("graph rebuild from cached railway data failed (%v), falling back to OSM parse", buildErr)
	}

	nodesByID := make(map[osm.NodeID]*osm.Node)
	lines := make(map[osm.WayID]RailwayLine)
	data, nodesByID, lines, err = parseRailwayData(path)
	if err != nil {
		return RailwayData{}, nil, err
	}

	graph, err := LoadOrBuildGraph(path, nodesByID, lines)
	if err != nil {
		return RailwayData{}, nil, err
	}

	if dataFromCacheFlag {
		log.Printf("refreshing stale railway data cache %s", dataCachePath)
	}
	if err := saveRailwayDataToCache(dataCachePath, data); err != nil {
		log.Printf("warning: failed to save railway data cache: %v", err)
	}

	return data, graph, nil
}

func rebuildNodesFromCachedData(data RailwayData) map[osm.NodeID]*osm.Node {
	nodesByID := make(map[osm.NodeID]*osm.Node, len(data.LinesByID)*8)
	for _, ln := range data.LinesByID {
		n := minInt(len(ln.NodeIDs), len(ln.Coords))
		for i := 0; i < n; i++ {
			nid := ln.NodeIDs[i]
			if _, ok := nodesByID[nid]; ok {
				continue
			}
			c := ln.Coords[i]
			nodesByID[nid] = &osm.Node{
				ID:  nid,
				Lat: c.Lat,
				Lon: c.Lon,
			}
		}
	}
	return nodesByID
}

func parseRailwayData(path string) (RailwayData, map[osm.NodeID]*osm.Node, map[osm.WayID]RailwayLine, error) {
	f, err := os.Open(path)
	if err != nil {
		return RailwayData{}, nil, nil, err
	}
	defer f.Close()

	ctx := context.Background()

	ext := strings.ToLower(filepath.Ext(path))
	var scanner interface {
		Scan() bool
		Object() osm.Object
		Err() error
	}
	if ext == ".pbf" {
		scanner = osmpbf.New(ctx, f, runtime.GOMAXPROCS(0))
	} else {
		scanner = osmxml.New(ctx, f)
	}

	stations := make(map[osm.NodeID]RailwayStation)
	lines := make(map[osm.WayID]RailwayLine)
	nodesByID := make(map[osm.NodeID]*osm.Node)

	for scanner.Scan() {
		obj := scanner.Object()
		switch v := obj.(type) {
		case *osm.Node:
			n := v
			nodesByID[n.ID] = n

			if !hasRailwayStationTag(n.Tags) {
				continue
			}
			name := n.Tags.Find("name")
			typ := stationTypeFromTags(n.Tags)
			tags := tagsToMap(n.Tags)
			stations[n.ID] = RailwayStation{
				ID:   n.ID,
				Name: name,
				Type: typ,
				Lat:  n.Lat,
				Lon:  n.Lon,
				Tags: tags,
			}
		case *osm.Way:
			w := v
			if !hasRailwayWayTag(w.Tags) {
				continue
			}
			name := w.Tags.Find("name")
			typ := wayTypeFromTags(w.Tags)

			nodeIDs := make([]osm.NodeID, 0, len(w.Nodes))
			for _, nd := range w.Nodes {
				nodeIDs = append(nodeIDs, nd.ID)
			}
			lines[w.ID] = RailwayLine{
				ID:      w.ID,
				Name:    name,
				Type:    typ,
				NodeIDs: nodeIDs,
				Coords:  nil, // populated after scanning, once nodesByID is complete
				Tags:    w.Tags,
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return RailwayData{}, nil, nil, err
	}

	for wayID, ln := range lines {
		coords := make([]struct {
			Lat float64 `json:"lat"`
			Lon float64 `json:"lon"`
		}, 0, len(ln.NodeIDs))
		for _, nid := range ln.NodeIDs {
			if n, ok := nodesByID[nid]; ok {
				coords = append(coords, struct {
					Lat float64 `json:"lat"`
					Lon float64 `json:"lon"`
				}{Lat: n.Lat, Lon: n.Lon})
			}
		}
		ln.Coords = coords
		lines[wayID] = ln
	}

	data := RailwayData{
		StationsByID: stations,
		LinesByID:    lines,
	}
	return data, nodesByID, lines, nil
}

func loadRailwayDataFromCache(path string) (RailwayData, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return RailwayData{}, false, err
	}
	defer f.Close()

	var data RailwayData
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&data); err != nil {
		return RailwayData{}, false, err
	}
	return data, true, nil
}

func saveRailwayDataToCache(path string, data RailwayData) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	if err := enc.Encode(data); err != nil {
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

func hasRailwayStationTag(tags osm.Tags) bool {
	if tags == nil {
		return false
	}
	if tags.Find("railway") == "station" || tags.Find("public_transport") == "station" {
		return true
	}
	switch tags.Find("railway") {
	case "halt", "stop", "junction":
		return true
	}
	return false
}

func stationTypeFromTags(tags osm.Tags) string {
	if tags == nil {
		return ""
	}
	if v := tags.Find("railway"); v != "" {
		return v
	}
	if v := tags.Find("public_transport"); v != "" {
		return v
	}
	return "station"
}

func hasRailwayWayTag(tags osm.Tags) bool {
	if tags == nil {
		return false
	}
	_, ok := allowedRailwayWayTypeSet[tags.Find("railway")]
	if ok {
		return true
	}
	return false
}

func wayTypeFromTags(tags osm.Tags) string {
	if tags == nil {
		return ""
	}
	if v := tags.Find("railway"); v != "" {
		return v
	}
	return "rail"
}

func getOSMPath() string {
	if p := os.Getenv("OSM_FILE"); p != "" {
		return p
	}
	// Default to ./data/map.osm relative to project root
	wd, err := os.Getwd()
	if err != nil {
		return "data/map.osm"
	}
	return filepath.Join(wd, "data", "map.osm")
}

func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.Trim(parts[1], "\"'")
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
}

func getEnv(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("failed to write json response: %v", err)
	}
}

func tagsToMap(tags osm.Tags) map[string]string {
	if tags == nil {
		return nil
	}
	out := make(map[string]string, len(tags))
	for _, t := range tags {
		k := strings.TrimSpace(t.Key)
		if k == "" {
			continue
		}
		out[k] = t.Value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildIFOPTIndex(data RailwayData) map[string][]osm.NodeID {
	idx := make(map[string][]osm.NodeID, len(data.StationsByID)/4+1)
	for id, st := range data.StationsByID {
		if st.Tags == nil {
			continue
		}
		v := strings.TrimSpace(st.Tags["ref:IFOPT"])
		if v == "" {
			continue
		}
		p := ifoptPrefix(v)
		if p == "" {
			continue
		}
		idx[p] = append(idx[p], id)
	}
	// Deterministic ordering.
	for k := range idx {
		ids := idx[k]
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		idx[k] = ids
	}
	return idx
}

func ifoptPrefix(s string) string {
	// Keep only first 3 elements: "de:xxx:yyy"
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	parts := strings.Split(s, ":")
	out := make([]string, 0, 3)
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, strings.ToLower(p))
		if len(out) == 3 {
			break
		}
	}
	if len(out) < 3 {
		return ""
	}
	return strings.Join(out, ":")
}

// (intentionally no generic readAllBody helper; handlers should read once)

func parseInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	var sign int64 = 1
	if s[0] == '-' {
		sign = -1
		s = s[1:]
	}
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not int")
		}
		n = n*10 + int64(c-'0')
	}
	return sign * n, nil
}

func parseIntDefault(s string, def int) int {
	n64, err := parseInt64(s)
	if err != nil {
		return def
	}
	if n64 > int64(math.MaxInt) {
		return def
	}
	if n64 < int64(math.MinInt) {
		return def
	}
	return int(n64)
}

func parseBoolDefault(s string, def bool) bool {
	v := strings.ToLower(strings.TrimSpace(s))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func parseFloat64(s string) (float64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func fileSignature(path string) (string, error) {
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	_, _ = h.Write([]byte(path))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(st.ModTime().UTC().Format(time.RFC3339Nano)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fmt.Sprintf("%d", st.Size())))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(cacheSchemaVersion))
	return hex.EncodeToString(h.Sum(nil))[:20], nil
}

func stopRefToTripStopSpec(s StopRef) TripStopSpec {
	out := TripStopSpec{}
	if strings.TrimSpace(s.IFOPT) != "" {
		v := strings.TrimSpace(s.IFOPT)
		out.IFOPT = &v
	}
	// Prefer station_id, but keep id alias too.
	if s.StationID != nil {
		out.StationID = s.StationID
	} else if s.ID != nil {
		out.ID = s.ID
	}
	out.Lat = s.Lat
	out.Lon = s.Lon
	return out
}

func (s *Server) resolveStopRef(raw StopRef) (ResolvedStop, bool) {
	// Reuse existing resolution rules by converting to TripStopSpec.
	spec := stopRefToTripStopSpec(raw)
	return s.resolveTripStop(spec)
}
