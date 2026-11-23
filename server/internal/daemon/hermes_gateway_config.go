package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/multica-ai/multica/server/internal/cli"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func loadHermesGatewayEntries(profile string, agents map[string]AgentEntry) error {
	dir, err := cli.ProfileDir(profile)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(dir, "hermes-gateways.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read Hermes Gateway configuration: %w", err)
	}
	return addHermesGatewayEntries(data, agents)
}

func addHermesGatewayEntries(data []byte, agents map[string]AgentEntry) error {
	var gateways map[string]agent.HermesGatewayConnection
	if err := json.Unmarshal(data, &gateways); err != nil || gateways == nil {
		return fmt.Errorf("hermes-gateways.json must contain an object of named Gateway connections")
	}
	configured := make(map[string]AgentEntry, len(gateways))
	for name, connection := range gateways {
		provider := agent.HermesGatewayProvider(name)
		if !agent.IsHermesGatewayProvider(provider) {
			return fmt.Errorf("invalid Hermes Gateway name %q: use lowercase letters, digits, and hyphens, starting with a letter", name)
		}
		if err := connection.Validate(); err != nil {
			return fmt.Errorf("Hermes Gateway %q: %w", name, err)
		}
		configured[provider] = AgentEntry{hermesGateway: connection}
	}
	for provider, entry := range configured {
		agents[provider] = entry
	}
	return nil
}
