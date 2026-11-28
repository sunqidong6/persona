package assistant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/nwverse/persona/server/internal/assistantwire"
	"github.com/nwverse/persona/server/internal/proto"
)

// A cursor is process-local: rebuilding it after a manager restart invalidates old
// watches. Take it before listing tools so a concurrent link cannot be missed.
type mcpToolChanges struct {
	mu      sync.Mutex
	cursor  string
	changed chan struct{}
}

func (c *mcpToolChanges) snapshot() (string, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.changed == nil {
		c.cursor, c.changed = randomHex(16), make(chan struct{})
	}
	return c.cursor, c.changed
}

func (c *mcpToolChanges) notify() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.changed != nil {
		close(c.changed)
	}
	c.cursor, c.changed = randomHex(16), make(chan struct{})
}

// mcpToolsHeartbeat is a discovery heartbeat, not a legacy startup wait. Revision-aware calls never use leaderListWait.
var mcpToolsHeartbeat = 30 * time.Second

// mcpToolsWatchEvent is a test-only observation hook. Production leaves it nil.
var mcpToolsWatchEvent func(string)

func noteMCPWatchEvent(event string) {
	if mcpToolsWatchEvent != nil {
		mcpToolsWatchEvent(event)
	}
}

type revisionDiscoveryKey struct{}

func (s *Service) sessionMCPToolsSince(ctx context.Context, machine, session string, assistant, guest bool, since string) map[string]any {
	if since != "" {
		ctx = context.WithValue(ctx, revisionDiscoveryKey{}, true)
	}
	ctx = s.mcpDiscoveryContext(ctx, machine, session, assistant)
	base, changed := s.mcpChanges.snapshot()
	decision := s.toolVisibilityDecision(ctx, machine, session, true)
	cursor := s.mcpVisibilityCursor(ctx, machine, session, assistant, base, decision)
	if since == cursor {
		noteMCPWatchEvent("wait")
		timer := time.NewTimer(mcpToolsHeartbeat)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return mcpDiscoveryUnchanged(cursor, decision, session)
		case <-timer.C:
			noteMCPWatchEvent("heartbeat")
			// A heartbeat also rechecks filter state: expiry and read recovery do not call notify themselves.
			base, _ = s.mcpChanges.snapshot()
			ctx = s.mcpDiscoveryContext(ctx, machine, session, assistant)
			decision = s.toolVisibilityDecision(ctx, machine, session, true)
			next := s.mcpVisibilityCursor(ctx, machine, session, assistant, base, decision)
			if next == cursor {
				return mcpDiscoveryUnchanged(cursor, decision, session)
			}
		case <-changed:
			base, _ = s.mcpChanges.snapshot()
		}
	}
	// Filter with a fresh capture and return its decision as the revision too. An admission read and a list read may
	// disagree transiently, but the list and revision a client caches always come from the same effective decision.
	ctx = s.mcpDiscoveryContext(ctx, machine, session, assistant)
	decision = s.toolVisibilityDecision(ctx, machine, session, true)
	tools, err := s.sessionMCPToolsVisible(ctx, machine, session, assistant, guest, decision)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	if tools == nil {
		tools = []proto.Tool{}
	}
	out := map[string]any{"tools": tools, "revision": s.mcpVisibilityCursor(ctx, machine, session, assistant, base, decision)}
	if session != "" || decision.Mode != assistantwire.ToolVisibilityOpen {
		out["tool_visibility"] = decision
	}
	return out
}

func mcpDiscoveryUnchanged(cursor string, decision assistantwire.ToolVisibilityDecision, session string) map[string]any {
	out := map[string]any{"revision": cursor, "unchanged": true}
	if session != "" || decision.Mode != assistantwire.ToolVisibilityOpen {
		out["tool_visibility"] = decision
	}
	return out
}

func (s *Service) mcpVisibilityCursor(ctx context.Context, machine, session string, assistant bool, base string, decision assistantwire.ToolVisibilityDecision) string {
	if decision.Mode == assistantwire.ToolVisibilityFailClosed {
		return base + ":visibility-unavailable"
	}
	base = s.guardDiscoveryCursor(ctx, machine, session, assistant, base)
	if decision.Mode == assistantwire.ToolVisibilityOpen {
		return base
	}
	encoded, _ := json.Marshal(decision)
	digest := sha256.Sum256(encoded)
	return base + ":visibility=" + hex.EncodeToString(digest[:16])
}

func (s *Service) mcpDiscoveryContext(ctx context.Context, machine, session string, assistant bool) context.Context {
	if assistant && machine != "" && session != "" {
		row, known := s.session(ctx, machine, session)
		ctx = context.WithValue(ctx, callerSpaceSnapshotKey{}, callerSpaceSnapshot{machine, session, row, known})
	}
	return context.WithValue(ctx, guardDecisionKey{}, &guardDecisionCapture{})
}
