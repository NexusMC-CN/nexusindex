package indexer

import (
	"context"
	"errors"
	"testing"
)

type fakeRebuildBackend struct {
	building  int64
	activated int64
	failed    int64
	buildErr  error
}

func (f *fakeRebuildBackend) CreateBuilding(context.Context, int64, int64) (int64, error) {
	f.building = 2
	return 2, nil
}
func (f *fakeRebuildBackend) Build(context.Context, int64) (int64, error)  { return 5, f.buildErr }
func (f *fakeRebuildBackend) Validate(context.Context, int64, int64) error { return nil }
func (f *fakeRebuildBackend) CatchUp(context.Context, int64, int64) error  { return nil }
func (f *fakeRebuildBackend) Activate(_ context.Context, id int64, count int64) error {
	f.activated = id
	return nil
}
func (f *fakeRebuildBackend) Fail(_ context.Context, id int64, _ error) error {
	f.failed = id
	return nil
}

func TestShadowRebuildActivatesOnlyAfterSuccessfulBuild(t *testing.T) {
	backend := &fakeRebuildBackend{}
	coordinator := NewRebuildCoordinator(backend)
	result, err := coordinator.Run(context.Background(), 11, 100)
	if err != nil {
		t.Fatal(err)
	}
	if result.GenerationID != 2 || result.Indexed != 5 {
		t.Fatalf("unexpected result: %#v", result)
	}
	if backend.activated != 2 || backend.failed != 0 {
		t.Fatalf("unexpected backend state: %#v", backend)
	}
}

func TestShadowRebuildFailureNeverActivatesBuildingGeneration(t *testing.T) {
	backend := &fakeRebuildBackend{buildErr: errors.New("source failed")}
	coordinator := NewRebuildCoordinator(backend)
	_, err := coordinator.Run(context.Background(), 11, 100)
	if err == nil {
		t.Fatal("expected rebuild failure")
	}
	if backend.activated != 0 || backend.failed != 2 {
		t.Fatalf("unexpected backend state: %#v", backend)
	}
}

func TestShadowRebuildRejectsConcurrentRun(t *testing.T) {
	coordinator := NewRebuildCoordinator(&fakeRebuildBackend{})
	coordinator.running.Store(true)
	_, err := coordinator.Run(context.Background(), 11, 100)
	if !errors.Is(err, ErrRebuildInProgress) {
		t.Fatalf("expected ErrRebuildInProgress, got %v", err)
	}
}
