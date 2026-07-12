package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/paulmach/osm"
)

func TestReadBoundedBodyRejectsOversizedRequest(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/route", strings.NewReader(strings.Repeat("x", (1<<20)+1)))
	response := httptest.NewRecorder()
	_, err := readBoundedBody(response, request)
	if err == nil {
		t.Fatal("expected oversized body to be rejected")
	}
}

func TestStationsEndpointIsDeterministicAndPaginated(t *testing.T) {
	server := &Server{data: RailwayData{StationsByID: map[osm.NodeID]RailwayStation{
		3: {ID: 3, Name: "Gamma"},
		1: {ID: 1, Name: "Alpha"},
		2: {ID: 2, Name: "Beta"},
	}}}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/stations?limit=2", nil)
	response := httptest.NewRecorder()
	server.handleStations(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.Code)
	}
	if response.Header().Get("X-Next-Cursor") != "2" {
		t.Fatalf("expected next cursor 2, got %q", response.Header().Get("X-Next-Cursor"))
	}
	if body := response.Body.String(); !strings.Contains(body, `"id":1`) || !strings.Contains(body, `"id":2`) || strings.Contains(body, `"id":3`) {
		t.Fatalf("unexpected response page: %s", body)
	}
}

func TestRouteContextHonorsCancellation(t *testing.T) {
	graph := &RailGraph{
		NodeCoords: [][2]float32{{48, 11}, {48.01, 11.01}},
		Adj: [][]Edge{
			{{To: 1, DistM: 100}},
			nil,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, _, _, ok, err := RouteContext(ctx, graph, 0, 1, "astar")
	if ok {
		t.Fatal("canceled route unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRouteLimitHonorsWaitingRequestCancellation(t *testing.T) {
	server := &Server{routeSem: make(chan struct{}, 1)}
	server.routeSem <- struct{}{}
	handler := server.withRouteLimit(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler should not run")
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/route", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	handler(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", response.Code)
	}
}
