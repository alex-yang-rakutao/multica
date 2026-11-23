package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const hermesGatewayProviderPrefix = "hermes_gateway-"

var hermesGatewayName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,39}$`)

func HermesGatewayProvider(name string) string {
	return hermesGatewayProviderPrefix + name
}

func IsHermesGatewayProvider(provider string) bool {
	name, ok := strings.CutPrefix(provider, hermesGatewayProviderPrefix)
	return ok && hermesGatewayName.MatchString(name)
}

type HermesGatewayConnection struct {
	URL    string `json:"url"`
	APIKey string `json:"api_key"`
}

func (c HermesGatewayConnection) Validate() error {
	u, err := url.Parse(c.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
		u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("expected an HTTP or HTTPS Gateway URL without credentials, query, or fragment")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Hostname() != "localhost" && !ip.IsLoopback() {
		return fmt.Errorf("expected a loopback Gateway URL")
	}
	if strings.TrimSpace(c.APIKey) == "" || strings.ContainsAny(c.APIKey, "\r\n") {
		return fmt.Errorf("expected a Gateway API key")
	}
	return nil
}

var hermesGatewayHTTP = &http.Client{
	Transport: &http.Transport{ResponseHeaderTimeout: 8 * time.Second},
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func (c HermesGatewayConnection) request(ctx context.Context, method, path, accept string, payload []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("invalid Hermes Gateway request")
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Accept", accept)
	if len(payload) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := hermesGatewayHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Hermes Gateway %s request failed", method)
	}
	return resp, nil
}

func (c HermesGatewayConnection) json(ctx context.Context, method, path string, payload []byte, out any) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	resp, err := c.request(ctx, method, path, "application/json", payload)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, fmt.Errorf("Hermes Gateway %s returned HTTP %d", method, resp.StatusCode)
	}
	if out != nil {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out); err != nil {
			return resp.StatusCode, fmt.Errorf("invalid JSON response from Hermes Gateway")
		}
	}
	return resp.StatusCode, nil
}

func (c HermesGatewayConnection) Probe(ctx context.Context) error {
	var capabilities struct {
		Auth struct {
			Required bool `json:"required"`
		} `json:"auth"`
		Endpoints map[string]struct {
			Path string `json:"path"`
		} `json:"endpoints"`
	}
	if _, err := c.json(ctx, http.MethodGet, "/v1/capabilities", nil, &capabilities); err != nil {
		return err
	}
	if !capabilities.Auth.Required {
		return fmt.Errorf("Hermes Gateway must require API key authentication")
	}
	if capabilities.Endpoints["runs"].Path != "/v1/runs" || capabilities.Endpoints["run_stop"].Path == "" {
		return fmt.Errorf("Hermes Gateway does not advertise the Runs API")
	}
	return nil
}

type hermesRemoteBackend struct {
	connection HermesGatewayConnection
	taskID     string
}

type hermesRemoteEvent struct {
	Event           string          `json:"event"`
	Status          string          `json:"status"`
	RunID           string          `json:"run_id"`
	SessionID       string          `json:"session_id"`
	Output          string          `json:"output"`
	Error           json.RawMessage `json:"error"`
	Delta           string          `json:"delta"`
	Text            string          `json:"text"`
	Tool            string          `json:"tool"`
	ToolCallID      string          `json:"tool_call_id"`
	Preview         string          `json:"preview"`
	AlreadyStreamed bool            `json:"already_streamed"`
}

func (b *hermesRemoteBackend) Execute(ctx context.Context, prompt string, opts ExecOptions) (*Session, error) {
	ctx, cancel := runContext(ctx, opts.Timeout)
	messages := make(chan Message, 32)
	results := make(chan Result, 1)
	var terminal atomic.Bool
	go func() {
		defer cancel()
		defer close(results)
		start := time.Now()
		result := b.run(ctx, prompt, opts, messages)
		result.DurationMs = time.Since(start).Milliseconds()
		terminal.Store(result.Status != "cleanup_unconfirmed")
		close(messages)
		results <- result
	}()
	return &Session{
		Messages: messages, Result: results, CleanupRequired: true, TerminalObserved: terminal.Load,
	}, nil
}

func (b *hermesRemoteBackend) run(ctx context.Context, prompt string, opts ExecOptions, messages chan<- Message) Result {
	if ctx.Err() != nil {
		return Result{Status: "cancelled"}
	}
	sessionID := opts.ResumeSessionID
	if sessionID == "" {
		sessionID = b.taskID
	}
	payload, _ := json.Marshal(struct {
		Input        string `json:"input"`
		Instructions string `json:"instructions,omitempty"`
		SessionID    string `json:"session_id,omitempty"`
	}{prompt, opts.SystemPrompt, sessionID})

	var created hermesRemoteEvent
	// Keep the short create request alive across task cancellation so we can
	// learn the remote run ID and stop it. Never replay an ambiguous create.
	status, err := b.connection.json(context.WithoutCancel(ctx), http.MethodPost, "/v1/runs", payload, &created)
	if err != nil || created.RunID == "" {
		if status >= 400 && status < 500 && status != http.StatusRequestTimeout {
			return Result{Status: "failed", Error: err.Error()}
		}
		return Result{Status: "cleanup_unconfirmed", Error: "Hermes Gateway did not confirm run creation. Check the Gateway for an active run before retrying."}
	}

	path := "/v1/runs/" + url.PathEscape(created.RunID)
	if created.SessionID != "" {
		sessionID = created.SessionID
	}
	if sessionID == "" {
		sessionID = created.RunID
	}
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	events := make(chan hermesRemoteEvent, 32)
	go b.stream(streamCtx, path, events)
	var incoming <-chan hermesRemoteEvent = events
	poll := time.NewTicker(2 * time.Second)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return b.stop(ctx, path, sessionID)
		case event, ok := <-incoming:
			if !ok {
				incoming = nil
				continue
			}
			if strings.HasPrefix(event.Event, "run.") {
				event.Status = strings.TrimPrefix(event.Event, "run.")
				if result, ok := b.terminalResult(event, sessionID); ok {
					return result
				}
			}
			b.emit(ctx, event, messages)
		case <-poll.C:
			var current hermesRemoteEvent
			if _, err := b.connection.json(ctx, http.MethodGet, path, nil, &current); err == nil {
				if result, ok := b.terminalResult(current, sessionID); ok {
					return result
				}
			}
		}
	}
}

func (b *hermesRemoteBackend) stop(ctx context.Context, path, sessionID string) Result {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_, _ = b.connection.json(cleanup, http.MethodPost, path+"/stop", nil, nil)
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()
	for {
		var state hermesRemoteEvent
		if _, err := b.connection.json(cleanup, http.MethodGet, path, nil, &state); err == nil {
			if result, ok := b.terminalResult(state, sessionID); ok {
				if result.Status == "cancelled" && ctx.Err() == context.DeadlineExceeded {
					result.Status = "timeout"
				}
				return result
			}
		}
		select {
		case <-cleanup.Done():
			return Result{Status: "cleanup_unconfirmed", SessionID: sessionID, Error: "Hermes Gateway did not confirm that the run stopped. Check the Gateway before retrying."}
		case <-poll.C:
		}
	}
}

func (b *hermesRemoteBackend) terminalResult(event hermesRemoteEvent, sessionID string) (Result, bool) {
	if event.SessionID != "" {
		sessionID = event.SessionID
	}
	status := event.Status
	switch status {
	case "completed", "failed", "cancelled":
	case "interrupted":
		status = "failed"
	default:
		return Result{}, false
	}
	var message string
	_ = json.Unmarshal(event.Error, &message)
	return Result{
		Status: status, SessionID: sessionID,
		Output: b.redact(event.Output), Error: b.redact(message),
	}, true
}

func (b *hermesRemoteBackend) redact(s string) string {
	return strings.ReplaceAll(s, b.connection.APIKey, "[redacted]")
}

func (b *hermesRemoteBackend) emit(ctx context.Context, event hermesRemoteEvent, messages chan<- Message) {
	var msg Message
	switch event.Event {
	case "message.delta":
		msg = Message{Type: MessageText, Content: b.redact(event.Delta)}
	case "message.interim":
		if event.AlreadyStreamed {
			return
		}
		msg = Message{Type: MessageText, Content: b.redact(event.Text)}
	case "tool.started":
		msg = Message{Type: MessageToolUse, Tool: b.redact(event.Tool), CallID: b.redact(event.ToolCallID)}
		if event.Preview != "" {
			msg.Input = map[string]any{"preview": b.redact(event.Preview)}
		}
	case "tool.completed", "tool.failed":
		msg = Message{Type: MessageToolResult, Tool: b.redact(event.Tool), CallID: b.redact(event.ToolCallID)}
	default:
		return
	}
	select {
	case messages <- msg:
	case <-ctx.Done():
	}
}

func (b *hermesRemoteBackend) stream(ctx context.Context, path string, events chan<- hermesRemoteEvent) {
	defer close(events)
	resp, err := b.connection.request(ctx, http.MethodGet, path+"/events", "text/event-stream", nil)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 16<<10), 4<<20)
	var eventName string
	var data strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event:"):
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if data.Len()+len(line) > 4<<20 {
				return
			}
			data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			data.WriteByte('\n')
		case line == "":
			var event hermesRemoteEvent
			err := json.Unmarshal([]byte(data.String()), &event)
			data.Reset()
			if event.Event == "" {
				event.Event = eventName
			}
			eventName = ""
			if err == nil {
				select {
				case events <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}
