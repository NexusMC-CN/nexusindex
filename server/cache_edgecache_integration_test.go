package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	edgecache "github.com/blockbridge/avmcbbs/apps/nexusindex/packages/edgecache-go-client"
)

func TestTieredCachePreservesNumbersAcrossRealEdgeCacheL2(t *testing.T) {
	baseURL := startEdgeCacheHTTPFixture(t)
	newCache := func() *TieredCache {
		return NewTieredCache(TieredCacheOptions{
			L2: NewEdgeCacheAdapter(edgecache.New(edgecache.Options{BaseURL: baseURL})),
		})
	}
	first, second := newCache(), newCache()
	ctx := context.Background()
	const value = `{"numbers":[9007199254740993,0.1234567890123456789,42,-7,1.25]}`
	key := "nexusindex:v1:query:precision"
	first.Set(ctx, key, json.RawMessage(value), 60)
	if got, ok := first.Get(ctx, key); !ok || string(got) != value {
		t.Fatalf("writer L1 = %s, hit = %t", got, ok)
	}
	if got, ok := second.Get(ctx, key); !ok || string(got) != value {
		t.Fatalf("second instance L2 = %s, hit = %t", got, ok)
	}
	if stats := second.Stats(); stats.L1Misses != 1 || stats.L2Hits != 1 {
		t.Fatalf("did not exercise L1 miss and L2 hit: %+v", stats)
	}
	if got, ok := second.Get(ctx, key); !ok || string(got) != value {
		t.Fatalf("promoted L1 = %s, hit = %t", got, ok)
	}
	if stats := second.Stats(); stats.L1Hits != 1 {
		t.Fatalf("did not exercise promoted L1: %+v", stats)
	}
}

// Build the sibling service's real handler in a temporary process instead of
// adding the EdgeCache server and its dependencies to the NexusIndex module.
func startEdgeCacheHTTPFixture(t *testing.T) string {
	t.Helper()
	executable := filepath.Join(t.TempDir(), "edgecache-http-fixture")
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	build := exec.CommandContext(ctx, "go", "build", "-o", executable, "./server/testdata/httpfixture")
	build.Dir = filepath.Join("..", "..", "edgecache")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build EdgeCache fixture: %v\n%s", err, output)
	}
	process := exec.CommandContext(ctx, executable)
	var stderr bytes.Buffer
	process.Stderr = &stderr
	stdout, err := process.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := process.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close()
		if err := process.Wait(); err != nil {
			t.Errorf("EdgeCache fixture: %v\n%s", err, stderr.String())
		}
	})
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() {
		t.Fatalf("EdgeCache fixture did not report URL: %v", scanner.Err())
	}
	address := scanner.Text()
	if !strings.HasPrefix(address, "http://127.0.0.1:") {
		t.Fatalf("unexpected EdgeCache fixture URL %q", address)
	}
	return address
}
