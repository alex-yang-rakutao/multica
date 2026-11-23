package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHermesRemoteRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Error("missing Gateway credentials")
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/runs":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["input"] != "Review the project" || body["session_id"] != "prior-session" {
				t.Errorf("unexpected input: %v", body)
			}
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"run_id":"run-1"}`)
		case "GET /v1/runs/run-1/events":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"event\":\"message.delta\",\"delta\":\"Reviewing\"}\n\n")
			fmt.Fprint(w, "data: {\"event\":\"run.completed\",\"output\":\"Reviewed\"}\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend, err := ResolveBackend("hermes_gateway-bruce", Config{
		HermesGateway: HermesGatewayConnection{URL: server.URL, APIKey: "test-key"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "Review the project", ExecOptions{ResumeSessionID: "prior-session"})
	if err != nil {
		t.Fatal(err)
	}
	var text string
	for event := range session.Messages {
		if event.Type == MessageText {
			text += event.Content
		}
	}
	result := <-session.Result
	if text != "Reviewing" || result.Status != "completed" || result.Output != "Reviewed" || result.SessionID != "prior-session" {
		t.Fatalf("text=%q result=%+v", text, result)
	}
}

func TestHermesRemoteCancelsUpstreamRun(t *testing.T) {
	streamOpened := make(chan struct{})
	stopRequested := make(chan struct{})
	var stopPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/runs":
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"run_id":"run-2"}`)
		case "GET /v1/runs/run-2/events":
			w.Header().Set("Content-Type", "text/event-stream")
			w.(http.Flusher).Flush()
			close(streamOpened)
			<-r.Context().Done()
		case "POST /v1/runs/run-2/stop":
			close(stopRequested)
			fmt.Fprint(w, `{"status":"stopping"}`)
		case "GET /v1/runs/run-2":
			select {
			case <-stopRequested:
				if stopPolls.Add(1) == 1 {
					fmt.Fprint(w, `{"status":"stopping"}`)
					return
				}
				fmt.Fprint(w, `{"status":"cancelled"}`)
			default:
				fmt.Fprint(w, `{"status":"running"}`)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend, err := ResolveBackend("hermes_gateway-alfred", Config{
		HermesGateway: HermesGatewayConnection{URL: server.URL, APIKey: "test-key"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "Prepare a report", ExecOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-streamOpened:
	case <-ctx.Done():
		t.Fatal("Gateway stream did not open")
	}
	cancel()
	select {
	case result := <-session.Result:
		if result.Status != "cancelled" || !session.TerminalObserved() {
			t.Fatalf("unexpected result: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Gateway stop did not settle")
	}
	select {
	case <-stopRequested:
	default:
		t.Fatal("remote stop was never requested")
	}
	if stopPolls.Load() < 2 {
		t.Fatal("stop was acknowledged before the Gateway reached a terminal status")
	}
}

func TestHermesRemoteUsesStatusAfterStreamCloses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/runs":
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprint(w, `{"run_id":"run-3"}`)
		case "GET /v1/runs/run-3/events":
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, ": stream closed\n\n")
		case "GET /v1/runs/run-3":
			fmt.Fprint(w, `{"status":"completed","session_id":"remote-session","output":"Recovered output"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	backend, err := ResolveBackend("hermes_gateway-bruce", Config{
		HermesGateway: HermesGatewayConnection{URL: server.URL, APIKey: "test-key"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "Review the project", ExecOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result := <-session.Result
	if result.Status != "completed" || result.Output != "Recovered output" || result.SessionID != "remote-session" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestHermesRemoteDoesNotReplayAmbiguousCreate(t *testing.T) {
	var creates atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/runs" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		creates.Add(1)
		http.Error(w, "response lost after dispatch", http.StatusBadGateway)
	}))
	defer server.Close()

	backend, err := ResolveBackend("hermes_gateway-bruce", Config{
		HermesGateway: HermesGatewayConnection{URL: server.URL, APIKey: "test-key"},
	})
	if err != nil {
		t.Fatal(err)
	}
	session, err := backend.Execute(t.Context(), "Review the project", ExecOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	result := <-session.Result
	if result.Status != "cleanup_unconfirmed" || session.TerminalObserved() || creates.Load() != 1 {
		t.Fatalf("creates=%d result=%+v", creates.Load(), result)
	}
}
