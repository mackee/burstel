package main

import (
	"math"
	"testing"
)

func TestComputeLatencyNearestRank(t *testing.T) {
	samples := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	got := computeLatency(samples)
	if got.Count != 10 {
		t.Fatalf("count = %d", got.Count)
	}
	if got.P50 != 5 || got.P95 != 10 || got.P99 != 10 || got.Max != 10 {
		t.Fatalf("percentiles = %+v", got)
	}
	if math.Abs(got.Mean-5.5) > 1e-9 {
		t.Fatalf("mean = %v", got.Mean)
	}
}

func TestComputeLatencyEmpty(t *testing.T) {
	got := computeLatency(nil)
	if got.Count != 0 || got.P95 != 0 {
		t.Fatalf("empty = %+v", got)
	}
}

func TestBuildRoutesWeightsSumToOne(t *testing.T) {
	cfg := trialConfig{errorRate: 0.05}
	routes := buildRoutes(cfg)
	sum := 0.0
	for _, r := range routes {
		sum += r.weight
	}
	if math.Abs(sum-1.0) > 1e-9 {
		t.Fatalf("weights sum = %v", sum)
	}
	if len(routes) != 3 {
		t.Fatalf("routes = %d", len(routes))
	}
}

func TestPickRouteAlwaysReturnsKnownRoute(t *testing.T) {
	routes := []routeSpec{
		{path: "/api/items/42", weight: 0.70},
		{path: "/api/summary", weight: 0.25},
		{path: "/api/error", weight: 0.05},
	}
	for i := 0; i < 1000; i++ {
		got := pickRoute(routes)
		known := false
		for _, r := range routes {
			if r.path == got {
				known = true
			}
		}
		if !known {
			t.Fatalf("unknown route %q", got)
		}
	}
}
