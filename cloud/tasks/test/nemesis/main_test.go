package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTriggerForTest(t *testing.T, path, value string) {
	t.Helper()
	temporary := path + ".new"
	if err := os.WriteFile(temporary, []byte(value), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
}

func TestRestartTriggerConsumesGenerationOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trigger")
	writeTriggerForTest(t, path, "0\n")
	trigger, err := newRestartTrigger(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		value string
		want  bool
	}{{"0", false}, {"1", true}, {"1", false}, {"2", true}, {"2", false}} {
		writeTriggerForTest(t, path, step.value)
		got, err := trigger.requested()
		if err != nil || got != step.want {
			t.Fatalf("value=%s: got (%t, %v), want %t", step.value, got, err, step.want)
		}
	}
	writeTriggerForTest(t, path, "1")
	if _, err := trigger.requested(); err == nil {
		t.Fatal("decreasing a trigger generation must fail")
	}
}

func TestRestartTriggerRejectsInvalidInputs(t *testing.T) {
	if trigger, err := newRestartTrigger(""); err != nil || trigger != nil {
		t.Fatalf("empty path must select random mode, got %v, %v", trigger, err)
	}
	if _, err := newRestartTrigger("relative-trigger"); err == nil {
		t.Fatal("relative trigger path must be rejected")
	}
	path := filepath.Join(t.TempDir(), "trigger")
	for _, value := range []string{"", "-1", "not-a-number", "18446744073709551616", "0000000000000000000000000000000000000000"} {
		writeTriggerForTest(t, path, value)
		if _, err := newRestartTrigger(path); err == nil {
			t.Fatalf("invalid trigger %q must be rejected", value)
		}
	}
	if _, err := newRestartTrigger(t.TempDir()); err == nil {
		t.Fatal("directory trigger must be rejected")
	}
}

func TestControlledRestartDisablesTimerAndWaitsForChild(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "trigger")
	writeTriggerForTest(t, path, "0")
	trigger, err := newRestartTrigger(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(newContext(), 3*time.Second)
	defer stop()
	childExit := make(chan error, 1)
	cancelled := make(chan struct{})
	done := make(chan error, 1)
	// Unblock the simulated cmd.Wait even if an assertion fails.
	t.Cleanup(func() {
		select {
		case childExit <- context.Canceled:
		default:
		}
	})
	restartLog := filepath.Join(directory, "restarts")
	go func() {
		done <- waitIteration(ctx, func() { close(cancelled) }, childExit, 0, 0, restartLog, trigger)
	}()
	// A zero random interval would restart immediately if controlled mode
	// accidentally kept its timer enabled.
	select {
	case <-cancelled:
		t.Fatal("controlled mode restarted without a new generation")
	case err := <-done:
		t.Fatalf("wait returned before trigger: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	writeTriggerForTest(t, path, "1")
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("new trigger generation did not cancel child")
	}
	select {
	case err := <-done:
		t.Fatalf("restart acknowledged before child exited: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := os.Stat(restartLog); !os.IsNotExist(err) {
		t.Fatalf("restart log must be written only after child exit, got %v", err)
	}
	childExit <- context.Canceled
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("wait did not finish after child exit")
	}
	info, err := os.Stat(restartLog)
	if err != nil || info.Size() == 0 {
		t.Fatalf("completed restart must be logged: %v", err)
	}
	if requested, err := trigger.requested(); requested || err != nil {
		t.Fatalf("one generation must not restart twice: %v, %v", requested, err)
	}
}

func TestRandomRestartModeStillRestarts(t *testing.T) {
	ctx, stop := context.WithTimeout(newContext(), time.Second)
	defer stop()
	childExit := make(chan error, 1)
	cancelled := false
	err := waitIteration(ctx, func() {
		cancelled = true
		childExit <- context.Canceled
	}, childExit, 0, 0, "", nil)
	if err != nil || !cancelled {
		t.Fatalf("random timer must still restart: cancelled=%t err=%v", cancelled, err)
	}
}

func TestUnexpectedChildExitIsNotRestarted(t *testing.T) {
	childExit := make(chan error, 1)
	expected := errors.New("unexpected child exit")
	childExit <- expected
	cancelled := false
	err := waitIteration(newContext(), func() { cancelled = true }, childExit, 3600, 3600, "", nil)
	if !errors.Is(err, expected) || cancelled {
		t.Fatalf("unexpected exit must be returned, got cancelled=%t err=%v", cancelled, err)
	}
}

func TestControlledChildExitWithoutTriggerFailsEvenWithZeroExitCode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trigger")
	writeTriggerForTest(t, path, "0")
	trigger, err := newRestartTrigger(path)
	if err != nil {
		t.Fatal(err)
	}
	childExit := make(chan error, 1)
	childExit <- nil
	if err := waitIteration(newContext(), func() {}, childExit, 0, 0, "", trigger); err == nil {
		t.Fatal("controlled mode must not silently restart a child that exited without a trigger")
	}
}
