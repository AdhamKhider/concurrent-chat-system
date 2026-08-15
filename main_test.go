package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func waitForOutput(t *testing.T, console *Console, expected string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		console.mu.Lock()
		output := consoleBuffer(console)
		console.mu.Unlock()
		if strings.Contains(output, expected) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	console.mu.Lock()
	output := consoleBuffer(console)
	console.mu.Unlock()
	t.Fatalf("timed out waiting for %q; output was:\n%s", expected, output)
}

func consoleBuffer(console *Console) string {
	buffer, ok := console.writer.(*bytes.Buffer)
	if !ok {
		return ""
	}
	return buffer.String()
}

func TestJoinMessageAndLeaveFlow(t *testing.T) {
	var output bytes.Buffer
	console := NewConsole(&output)
	server := NewChatServer(console)
	server.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}
	}()

	if err := server.Join(ctx, NewClient("sara")); err != nil {
		t.Fatalf("joining sara failed: %v", err)
	}
	if err := server.Join(ctx, NewClient("mohamed")); err != nil {
		t.Fatalf("joining mohamed failed: %v", err)
	}

	waitForOutput(t, console, "[sara] User mohamed joined the chat.")

	if err := server.Send(ctx, "sara", "hello"); err != nil {
		t.Fatalf("sending message failed: %v", err)
	}
	waitForOutput(t, console, "[mohamed] sara: hello")

	console.mu.Lock()
	outputAfterMessage := consoleBuffer(console)
	console.mu.Unlock()
	if strings.Contains(outputAfterMessage, "[sara] sara: hello") {
		t.Fatalf("sender received its own message: %s", outputAfterMessage)
	}

	removed, err := server.Leave(ctx, "mohamed")
	if err != nil {
		t.Fatalf("removing mohamed failed: %v", err)
	}
	if removed != "mohamed" {
		t.Fatalf("removed user = %q, want mohamed", removed)
	}
	waitForOutput(t, console, "[sara] User mohamed left the chat.")
}

func TestDuplicateUsernameIsRejected(t *testing.T) {
	console := NewConsole(&bytes.Buffer{})
	server := NewChatServer(console)
	server.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}
	}()

	if err := server.Join(ctx, NewClient("Alice")); err != nil {
		t.Fatalf("joining Alice failed: %v", err)
	}
	if err := server.Join(ctx, NewClient("alice")); err != errDuplicateUsername {
		t.Fatalf("duplicate join error = %v, want %v", err, errDuplicateUsername)
	}
	if got := server.ListUsers(); len(got) != 1 || got[0] != "Alice" {
		t.Fatalf("connected users = %v, want [Alice]", got)
	}
}

func TestConcurrentJoinsAndLists(t *testing.T) {
	console := NewConsole(&bytes.Buffer{})
	server := NewChatServer(console)
	server.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}
	}()

	var joins sync.WaitGroup
	for i := 0; i < 10; i++ {
		joins.Add(1)
		go func(index int) {
			defer joins.Done()
			name := "user" + string(rune('0'+index))
			if err := server.Join(ctx, NewClient(name)); err != nil {
				t.Errorf("join %s failed: %v", name, err)
			}
		}(i)
	}
	joins.Wait()

	users := server.ListUsers()
	if len(users) != 10 {
		t.Fatalf("connected user count = %d, want 10", len(users))
	}
}
