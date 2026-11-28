package assistantwire

import (
	"context"
	"slices"
	"time"

	"github.com/nwverse/persona/server/internal/compat"
	"github.com/nwverse/persona/server/internal/proto"
	"github.com/nwverse/persona/server/internal/wireids"
)

type connectingDiscoveryKey struct{}

// WithConnectingDiscovery marks a node that can discover tools during its handshake but is not yet
// a target for a policy invalidation.
func WithConnectingDiscovery(ctx context.Context) context.Context {
	return context.WithValue(ctx, connectingDiscoveryKey{}, true)
}

func IsConnectingDiscovery(ctx context.Context) bool {
	connecting, _ := ctx.Value(connectingDiscoveryKey{}).(bool)
	return connecting
}

const (
	NodeMCPTools                = "mcp.tools"
	NodeMCPVisibilityInvalidate = "mcp.visibility.invalidate"
	ToolVisibilityRoute         = "/api/v1/assistant/tool-visibility"
	MCPWorkSearchTool           = "work_tools_search"
)

type ToolVisibilityRole string

const (
	ToolRoleMain       ToolVisibilityRole = "main"
	ToolRoleLeader     ToolVisibilityRole = "leader"
	ToolRoleBackground ToolVisibilityRole = "background"
	ToolRoleOrdinary   ToolVisibilityRole = "ordinary"
)

func (r ToolVisibilityRole) Valid() bool {
	switch r {
	case ToolRoleMain, ToolRoleLeader, ToolRoleBackground, ToolRoleOrdinary:
		return true
	default:
		return false
	}
}

// ToolVisibilityRule restricts a role or session until ExpiresAt. A nil Allowlist leaves all tools eligible.
// An explicit empty Allowlist allows none.
type ToolVisibilityRule struct {
	Allowlist []string  `json:"allowlist,omitzero"`
	Denylist  []string  `json:"denylist,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

type SessionToolVisibility struct {
	wireids.AgentRef
	ToolVisibilityRule
}

// ToolVisibilityPolicies is the owner's settings document. It is never part of the extension hello.
type ToolVisibilityPolicies struct {
	Roles    map[ToolVisibilityRole]ToolVisibilityRule `json:"roles,omitempty"`
	Sessions []SessionToolVisibility                   `json:"sessions,omitempty"`
}

type ToolVisibilityMode string

const (
	ToolVisibilityOpen         ToolVisibilityMode = "open"
	ToolVisibilityCheck        ToolVisibilityMode = "check"
	ToolVisibilityUnattributed ToolVisibilityMode = "unattributed"
	ToolVisibilityFailClosed   ToolVisibilityMode = "fail_closed"
)

// ToolVisibilityDecision carries the effective filters for one discovery. A node can reuse Open until invalidated.
// Check and FailClosed are refreshed on the next local call to handle changes of role, expiry and store recovery.
type ToolVisibilityDecision struct {
	Mode      ToolVisibilityMode `json:"mode"`
	Allowlist []string           `json:"allowlist,omitzero"`
	Denylist  []string           `json:"denylist,omitempty"`
}

func (d ToolVisibilityDecision) Allows(name string) bool {
	switch d.Mode {
	case ToolVisibilityOpen:
		return true
	case ToolVisibilityCheck:
		name = compat.MCPToolIdentity(name)
		return !slices.Contains(d.Denylist, name) && (d.Allowlist == nil || slices.Contains(d.Allowlist, name))
	default:
		return false
	}
}

func (d ToolVisibilityDecision) Filter(tools []proto.Tool) []proto.Tool {
	if d.Mode == ToolVisibilityOpen || d.Mode == ToolVisibilityUnattributed {
		return tools
	}
	if d.Mode != ToolVisibilityCheck {
		return []proto.Tool{}
	}
	visible := make([]proto.Tool, 0, len(tools))
	for _, tool := range tools {
		if d.Allows(tool.Name) {
			visible = append(visible, tool)
		}
	}
	return visible
}

// MCPToolsQuery is a node's discovery request. WithVisibility opts into decision metadata used only by that node.
// VisibilityOnly skips catalog work on an attributed local call whose decision needs to be checked.
type MCPToolsQuery struct {
	// These caller fields are explicit on the wire, including for an unattributed non-assistant caller.
	SessionID      wireids.SessionID `json:"session_id"`
	Assistant      bool              `json:"assistant"`
	Guest          bool              `json:"guest"`
	CallerCwd      string            `json:"caller_cwd,omitempty"`
	Revision       string            `json:"revision,omitempty"`
	ToolSet        string            `json:"tool_set,omitempty"`
	LeaderOnly     bool              `json:"leader_only,omitempty"`
	WithVisibility bool              `json:"with_visibility,omitempty"`
	VisibilityOnly bool              `json:"visibility_only,omitempty"`
}

type MCPToolsReply struct {
	Tools          []proto.Tool            `json:"tools,omitzero"`
	Revision       string                  `json:"revision,omitempty"`
	Unchanged      bool                    `json:"unchanged,omitempty"`
	Error          MCPDiscoveryFailure     `json:"error,omitempty"`
	ToolVisibility *ToolVisibilityDecision `json:"tool_visibility,omitempty"`
}

// MCPLocalToolsReply is the node.sock reply. The MCP process synthesizes Work Guide search from the listed tools.
type MCPLocalToolsReply struct {
	MCPToolsReply
	Session        wireids.SessionID `json:"session"`
	HideWorkSearch bool              `json:"hide_work_search,omitempty"`
}

type MCPDiscoveryFailure string
