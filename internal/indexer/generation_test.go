package indexer

import (
	"errors"
	"testing"
)

func TestGenerationTransitions(t *testing.T) {
	tests := []struct {
		from GenerationStatus
		to   GenerationStatus
		want bool
	}{
		{GenerationBuilding, GenerationActive, true},
		{GenerationBuilding, GenerationFailed, true},
		{GenerationActive, GenerationRetired, true},
		{GenerationRetired, GenerationActive, true},
		{GenerationFailed, GenerationActive, false},
		{GenerationActive, GenerationFailed, false},
	}
	for _, tt := range tests {
		if got := canTransitionGeneration(tt.from, tt.to); got != tt.want {
			t.Fatalf("transition %s -> %s: got %v want %v", tt.from, tt.to, got, tt.want)
		}
	}
}

func TestRequireActiveGenerationRejectsMissingGeneration(t *testing.T) {
	_, err := requireActiveGeneration(nil)
	if !errors.Is(err, ErrIndexUnavailable) {
		t.Fatalf("expected ErrIndexUnavailable, got %v", err)
	}
}

func TestRequireActiveGenerationReturnsActiveID(t *testing.T) {
	generation := &Generation{ID: 42, Status: GenerationActive}
	id, err := requireActiveGeneration(generation)
	if err != nil {
		t.Fatal(err)
	}
	if id != 42 {
		t.Fatalf("got generation %d", id)
	}
}

func TestRequireActiveGenerationRejectsNonActiveState(t *testing.T) {
	_, err := requireActiveGeneration(&Generation{ID: 7, Status: GenerationBuilding})
	if !errors.Is(err, ErrIndexUnavailable) {
		t.Fatalf("expected ErrIndexUnavailable, got %v", err)
	}
}
