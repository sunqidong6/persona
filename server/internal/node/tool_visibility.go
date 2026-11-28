package node

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nwverse/persona/server/internal/assistantwire"
	"github.com/nwverse/persona/server/internal/i18n"
	"github.com/nwverse/persona/server/internal/proto"
	"github.com/nwverse/persona/server/internal/wireids"
)

type toolVisibilityCache struct {
	mu        sync.Mutex
	epoch     uint64
	transport uint64
	decisions map[string]assistantwire.ToolVisibilityDecision
}

func (n *Node) invalidateToolVisibility() {
	c := &n.toolVisibility
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	c.decisions = nil
}

// visibilityEpochLocked also notices transport resumes, when the same Node and runtimes remain in use.
func (n *Node) visibilityEpochLocked() (uint64, bool) {
	c := &n.toolVisibility
	if n.peer == nil {
		return c.epoch, false
	}
	transport, live := n.peer.TransportEpoch()
	if !live || transport != c.transport {
		c.epoch++
		c.transport = transport
		c.decisions = nil
	}
	return c.epoch, live
}

func (n *Node) discoverMCP(ctx context.Context, params assistantwire.MCPToolsQuery) (assistantwire.MCPToolsReply, error) {
	session := params.SessionID.String()
	c := &n.toolVisibility
	c.mu.Lock()
	epoch, live := n.visibilityEpochLocked()
	c.mu.Unlock()
	if !live {
		return assistantwire.MCPToolsReply{}, errors.New("Persona hub is not connected")
	}
	var res assistantwire.MCPToolsReply
	if err := n.peer.Call(ctx, assistantwire.NodeMCPTools, params, &res); err != nil {
		return res, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current, live := n.visibilityEpochLocked()
	if !live || epoch != current {
		return assistantwire.MCPToolsReply{}, i18n.Err("assistant-tool_visibility.unavailable")
	}
	if n.managerToolVisibility.Load() && (session != "" || params.VisibilityOnly) && res.Error == "" && res.ToolVisibility == nil {
		return assistantwire.MCPToolsReply{}, i18n.Err("assistant-tool_visibility.unavailable")
	}
	if session != "" && res.Error == "" {
		if c.decisions == nil {
			c.decisions = make(map[string]assistantwire.ToolVisibilityDecision)
		}
		c.decisions[session] = discoveryVisibility(res)
	}
	return res, nil
}

func discoveryVisibility(res assistantwire.MCPToolsReply) assistantwire.ToolVisibilityDecision {
	if res.ToolVisibility == nil {
		// Older managers do not send a decision and have no visibility policy.
		return assistantwire.ToolVisibilityDecision{Mode: assistantwire.ToolVisibilityOpen}
	}
	return *res.ToolVisibility
}

func (n *Node) localToolVisible(ctx context.Context, session, name string) bool {
	c := &n.toolVisibility
	c.mu.Lock()
	_, live := n.visibilityEpochLocked()
	decision := c.decisions[session]
	c.mu.Unlock()
	if !live {
		return false
	}
	if decision.Mode == assistantwire.ToolVisibilityOpen {
		return true
	}
	// Recheck missing, restricted and fail-closed decisions. An empty session never borrows any cached decision.
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	res, err := n.discoverMCP(ctx, assistantwire.MCPToolsQuery{
		SessionID: wireids.SessionID(session), VisibilityOnly: true,
	})
	return err == nil && res.Error == "" && discoveryVisibility(res).Allows(name)
}

func toolVisibilityDenied() proto.ToolResult {
	return proto.TextResult(i18n.Err("assistant-tool_visibility.refused").Error(), true)
}
