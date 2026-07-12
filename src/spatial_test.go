package main

import (
	"sync"
	"testing"
)

func TestGridIndexKNearestConcurrent(t *testing.T) {
	coords := make([][2]float32, 0, 400)
	for y := 0; y < 20; y++ {
		for x := 0; x < 20; x++ {
			coords = append(coords, [2]float32{48 + float32(y)/1000, 11 + float32(x)/1000})
		}
	}
	index := NewGridIndex(coords, 0.01)
	var wait sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < 100; iteration++ {
				results := index.KNearest(48.005+float64(worker%5)/10000, 11.005, 5)
				if len(results) != 5 {
					t.Errorf("expected five nearest nodes, got %d", len(results))
					return
				}
			}
		}(worker)
	}
	wait.Wait()
}
