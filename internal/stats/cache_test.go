package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheRecordConcurrent fires many goroutines all calling Record at once.
// Without a write lock in Record this will be caught by the race detector:
//
//	go test -race ./internal/stats/
func TestCacheRecordConcurrent(t *testing.T) {
	c := stats.NewCache()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			c.Record("acc_concurrent", 1)
		}()
	}
	wg.Wait()

	got := c.Get("acc_concurrent")
	if got.CallCount != goroutines {
		t.Fatalf("CallCount = %d, want %d", got.CallCount, goroutines)
	}
	if got.TotalDurationSec != goroutines {
		t.Fatalf("TotalDurationSec = %d, want %d", got.TotalDurationSec, goroutines)
	}
}
