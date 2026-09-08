package main

import "testing"

func TestParseInvocationDefaultsToServe(t *testing.T) {
	invocation, err := parseInvocation(nil)
	if err != nil {
		t.Fatal(err)
	}
	if invocation.mode != modeServe {
		t.Fatalf("expected serve mode, got %q", invocation.mode)
	}
}

func TestParseInvocationAcceptsMigrationCommands(t *testing.T) {
	for _, action := range []string{"status", "up"} {
		invocation, err := parseInvocation([]string{"migrate", action})
		if err != nil {
			t.Fatalf("parse migrate %s: %v", action, err)
		}
		if invocation.mode != modeMigrate || invocation.action != action {
			t.Fatalf("unexpected invocation: %#v", invocation)
		}
	}
}

func TestParseInvocationRejectsUnknownCommands(t *testing.T) {
	for _, args := range [][]string{{"unknown"}, {"migrate"}, {"migrate", "down"}} {
		if _, err := parseInvocation(args); err == nil {
			t.Fatalf("expected args %v to be rejected", args)
		}
	}
}
