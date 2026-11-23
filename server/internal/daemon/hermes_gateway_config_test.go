package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHermesGatewayNamedRuntimeRegistration(t *testing.T) {
	gateway := func(key string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/capabilities" || r.Header.Get("Authorization") != "Bearer "+key {
				t.Errorf("invalid Gateway probe for %s", key)
				http.Error(w, "invalid probe", http.StatusUnauthorized)
				return
			}
			fmt.Fprint(w, `{"auth":{"required":true},"endpoints":{"runs":{"path":"/v1/runs"},"run_stop":{"path":"/v1/runs/{run_id}/stop"}}}`)
		}))
	}
	alfred := gateway("alfred-key")
	defer alfred.Close()
	bruce := gateway("bruce-key")
	defer bruce.Close()
	agents := map[string]AgentEntry{}
	cfg := fmt.Sprintf(`{"alfred":{"url":%q,"api_key":"alfred-key"},"bruce":{"url":%q,"api_key":"bruce-key"}}`, alfred.URL, bruce.URL)
	if err := addHermesGatewayEntries([]byte(cfg), agents); err != nil {
		t.Fatal(err)
	}
	d := &Daemon{cfg: Config{Agents: agents}, logger: slog.Default(), agentVersions: map[string]string{}}
	runtimes, _, _ := d.detectBuiltinRuntimes(context.Background())
	if len(runtimes) != 2 || runtimes[0]["type"] != "hermes_gateway-alfred" || runtimes[1]["type"] != "hermes_gateway-bruce" {
		t.Fatalf("expected both Gateway runtimes, got %v", runtimes)
	}
}
