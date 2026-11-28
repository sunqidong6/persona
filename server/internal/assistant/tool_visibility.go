package assistant

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nwverse/persona/server/internal/apierr"
	"github.com/nwverse/persona/server/internal/assistantwire"
	"github.com/nwverse/persona/server/internal/compat"
	"github.com/nwverse/persona/server/internal/i18n"
	"github.com/nwverse/persona/server/internal/wireids"
	"golang.org/x/sync/errgroup"
)

const toolVisibilityKey = "assistant.tool_visibility"

type toolVisibilityBook struct {
	change  sync.Mutex
	current atomic.Pointer[assistantwire.ToolVisibilityPolicies]
}

// transitioning is a distinct pointer. Discovery during invalidation must not
// put the old policy back in the node's newly cleared cache.
var transitioningToolVisibility = &assistantwire.ToolVisibilityPolicies{}

func (s *Service) readToolVisibility(ctx context.Context) (*assistantwire.ToolVisibilityPolicies, error) {
	var policy *assistantwire.ToolVisibilityPolicies
	if st := s.store(); st != nil {
		found, err := st.Setting(ctx, toolVisibilityKey, &policy)
		if err != nil {
			return nil, err
		}
		if found && policy == nil {
			return nil, errors.New("stored MCP tool visibility policy is null")
		}
	}
	if policy == nil {
		policy = &assistantwire.ToolVisibilityPolicies{}
	}
	if err := normalizeToolVisibility(policy); err != nil {
		return nil, err
	}
	return policy, nil
}

func (s *Service) toolVisibilityPolicies(ctx context.Context) (*assistantwire.ToolVisibilityPolicies, bool) {
	book := &s.toolVisibility
	policy := book.current.Load()
	if policy == nil {
		fresh, err := s.readToolVisibility(ctx)
		if err == nil {
			book.current.CompareAndSwap(nil, fresh)
		} else {
			s.logf("mcp visibility: read settings: %v", err)
		}
		policy = book.current.Load()
	}
	return policy, policy != nil && policy != transitioningToolVisibility
}

func normalizeVisibilityRule(rule assistantwire.ToolVisibilityRule) (assistantwire.ToolVisibilityRule, error) {
	normalize := func(names []string) ([]string, error) {
		if names == nil {
			return nil, nil
		}
		out := make([]string, 0, len(names))
		for _, name := range names {
			if name == "" || strings.ContainsFunc(name, func(r rune) bool {
				return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.')
			}) {
				return nil, fmt.Errorf("invalid MCP tool name %q", name)
			}
			out = append(out, compat.MCPToolIdentity(name))
		}
		slices.Sort(out)
		return slices.Compact(out), nil
	}
	var err error
	rule.Allowlist, err = normalize(rule.Allowlist)
	if err != nil {
		return rule, err
	}
	rule.Denylist, err = normalize(rule.Denylist)
	return rule, err
}

func normalizeToolVisibility(p *assistantwire.ToolVisibilityPolicies) error {
	for role, rule := range p.Roles {
		if !role.Valid() {
			return fmt.Errorf("unknown conversation role %q", role)
		}
		normalized, err := normalizeVisibilityRule(rule)
		if err != nil {
			return err
		}
		p.Roles[role] = normalized
	}
	seen := make(map[wireids.AgentRef]bool, len(p.Sessions))
	for i := range p.Sessions {
		entry := &p.Sessions[i]
		if entry.Machine == "" || entry.Session == "" ||
			strings.TrimSpace(entry.Machine.String()) != entry.Machine.String() ||
			strings.TrimSpace(entry.Session.String()) != entry.Session.String() {
			return errors.New("a session policy must name its machine and session")
		}
		if seen[entry.AgentRef] {
			return errors.New("duplicate MCP tool visibility session")
		}
		seen[entry.AgentRef] = true
		rule, err := normalizeVisibilityRule(entry.ToolVisibilityRule)
		if err != nil {
			return err
		}
		entry.ToolVisibilityRule = rule
	}
	return nil
}

var (
	visibilityOpen       = assistantwire.ToolVisibilityDecision{Mode: assistantwire.ToolVisibilityOpen}
	visibilityFailClosed = assistantwire.ToolVisibilityDecision{Mode: assistantwire.ToolVisibilityFailClosed}
)

// Visibility is an additional restriction on the caller's existing tool authorization.
// Session-less discovery is a registration catalog and confers no call authority.
func (s *Service) toolVisibilityDecision(ctx context.Context, machine, session string, discovery bool) (decision assistantwire.ToolVisibilityDecision) {
	if session != "" && assistantwire.IsConnectingDiscovery(ctx) {
		defer func() {
			if decision.Mode == assistantwire.ToolVisibilityOpen {
				decision.Mode = assistantwire.ToolVisibilityCheck
			}
		}()
	}
	policy, ok := s.toolVisibilityPolicies(ctx)
	if !ok {
		return visibilityFailClosed
	}
	if len(policy.Roles) == 0 && len(policy.Sessions) == 0 {
		return visibilityOpen
	}
	now := time.Now()
	roleRules := hasActiveRoleVisibility(policy, now)
	hasSessionRules := slices.ContainsFunc(policy.Sessions, func(rule assistantwire.SessionToolVisibility) bool {
		return activeVisibilityRule(rule.ToolVisibilityRule, now)
	})
	if !roleRules && !hasSessionRules {
		return visibilityOpen
	}
	if session == "" || machine == "" {
		if discovery {
			return visibilityOpen
		}
		return visibilityFailClosed
	}

	// A running session can change roles or become the active segment of a logical conversation.
	decision.Mode = assistantwire.ToolVisibilityCheck
	if roleRules {
		role, known := s.toolVisibilityRole(ctx, machine, session)
		if !known {
			return visibilityFailClosed
		}
		applyVisibilityRule(&decision, policy.Roles[role], now)
	}
	machine = s.canonical(machine)
	for _, rule := range policy.Sessions {
		if rule.Session.String() == session && s.canonical(rule.Machine.String()) == machine {
			applyVisibilityRule(&decision, rule.ToolVisibilityRule, now)
		}
	}
	return decision
}

func hasActiveRoleVisibility(policy *assistantwire.ToolVisibilityPolicies, now time.Time) bool {
	for _, rule := range policy.Roles {
		if activeVisibilityRule(rule, now) {
			return true
		}
	}
	return false
}

func activeVisibilityRule(rule assistantwire.ToolVisibilityRule, now time.Time) bool {
	return (rule.Allowlist != nil || len(rule.Denylist) > 0) && (rule.ExpiresAt.IsZero() || now.Before(rule.ExpiresAt))
}

// All applicable allowlists must agree. Any matching denylist entry wins, including across scopes.
func applyVisibilityRule(d *assistantwire.ToolVisibilityDecision, rule assistantwire.ToolVisibilityRule, now time.Time) {
	if !activeVisibilityRule(rule, now) {
		return
	}
	d.Mode = assistantwire.ToolVisibilityCheck
	if rule.Allowlist != nil {
		if d.Allowlist == nil {
			d.Allowlist = slices.Clone(rule.Allowlist)
		} else {
			d.Allowlist = slices.DeleteFunc(d.Allowlist, func(name string) bool {
				return !slices.Contains(rule.Allowlist, name)
			})
		}
	}
	d.Denylist = append(d.Denylist, rule.Denylist...)
}

func (s *Service) toolVisibilityRole(ctx context.Context, machine, session string) (assistantwire.ToolVisibilityRole, bool) {
	cfg, ok := s.settingsLastGood(ctx)
	if !ok {
		return "", false
	}
	cfg = s.effective(ctx, cfg)
	sameMachine := func(expected string) bool {
		if s.sameMachine(machine, expected) {
			return true
		}
		// The logical caller has already been mapped to the root by the tool entry point.
		caller, ok := ctx.Value(logicalAuthenticatedKey{}).(logicalAuthenticated)
		return ok && caller.Machine == machine && caller.Session == session && s.sameMachine(caller.PhysicalMachine, expected)
	}
	switch {
	case cfg.Main == session && (sameMachine(cfg.mainRouteMachine()) || sameMachine(s.confirmedMainMachine(ctx, cfg))):
		return assistantwire.ToolRoleMain, true
	case cfg.BackgroundSupervisor == session && sameMachine(cmp.Or(cfg.BackgroundSupervisorMachineID, cfg.MachineID)),
		s.isBackgroundCallerOn(ctx, machine, session):
		return assistantwire.ToolRoleBackground, true
	case slices.Contains(cfg.Leaders, session):
		direction, _, loaded := s.directionSnapshot(ctx, session)
		if !loaded {
			return "", false
		}
		if direction.Outside == nil && sameMachine(cmp.Or(direction.MachineID, cfg.MachineID)) {
			return assistantwire.ToolRoleLeader, true
		}
	}
	return assistantwire.ToolRoleOrdinary, true
}

func (s *Service) toolVisibilityRefusal(ctx context.Context, machine, session, name string) error {
	if s.toolVisibilityDecision(ctx, machine, session, false).Allows(name) {
		return nil
	}
	return i18n.Err("assistant-tool_visibility.refused")
}

func (s *Service) invalidateNodeToolVisibility(ctx context.Context) error {
	if s.devs == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var group errgroup.Group
	group.SetLimit(8)
	for _, id := range s.devs.ConnectedNodeIDs() {
		group.Go(func() error {
			err := s.nodeCall(ctx, id, assistantwire.NodeMCPVisibilityInvalidate, struct{}{}, nil)
			if err == nil || err.Error() == "unknown method" || err.Error() == "unknown method "+assistantwire.NodeMCPVisibilityInvalidate {
				return nil
			}
			return fmt.Errorf("invalidate MCP visibility on %s: %w", id, err)
		})
	}
	return group.Wait()
}

func (s *Service) saveToolVisibility(ctx context.Context, policy *assistantwire.ToolVisibilityPolicies) error {
	st := s.store()
	if st == nil {
		return errors.New("MCP tool visibility requires the settings store")
	}
	book := &s.toolVisibility
	book.change.Lock()
	defer book.change.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	before := book.current.Swap(transitioningToolVisibility)
	defer s.mcpChanges.notify()
	if err := s.invalidateNodeToolVisibility(ctx); err != nil {
		book.current.Store(before)
		return err
	}
	if err := st.PutSetting(ctx, toolVisibilityKey, policy); err != nil {
		book.current.Store(before)
		return err
	}
	book.current.Store(policy)
	return nil
}

// refreshToolVisibility serializes a hub hello with policy writes and rediscovery.
// Keep the last good policy if a refresh fails, even when the hello itself is applied.
func (s *Service) refreshToolVisibility(ctx context.Context, current func() bool, apply func()) bool {
	book := &s.toolVisibility
	book.change.Lock()
	defer book.change.Unlock()
	if !current() {
		return false
	}
	before := book.current.Swap(transitioningToolVisibility)
	err := s.invalidateNodeToolVisibility(ctx)
	apply()
	s.prefs.mu.Lock()
	s.prefs.at = time.Time{}
	s.prefs.gen++
	s.prefs.mu.Unlock()
	if err == nil {
		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		fresh, readErr := s.readToolVisibility(readCtx)
		cancel()
		if readErr == nil {
			before = fresh
		} else {
			err = readErr
		}
	}
	book.current.Store(before)
	s.mcpChanges.notify()
	if err != nil {
		s.logf("mcp visibility: refresh: %v", err)
	}
	return true
}

type toolVisibilitySetting struct {
	ToolVisibility *assistantwire.ToolVisibilityPolicies `json:"tool_visibility"`
}

func (s *Service) toolVisibilityRoutes() {
	s.handle("GET "+assistantwire.ToolVisibilityRoute, func(w http.ResponseWriter, r *http.Request) {
		if !fullRole(r) {
			apierr.Write(w, http.StatusForbidden, i18n.Err("assistant-tool_visibility.owner_only"))
			return
		}
		policy, ok := s.toolVisibilityPolicies(r.Context())
		if !ok {
			apierr.Write(w, http.StatusServiceUnavailable, i18n.Err("assistant-tool_visibility.unavailable"))
			return
		}
		apierr.WriteJSON(w, toolVisibilitySetting{ToolVisibility: policy})
	})
	s.handle("PUT "+assistantwire.ToolVisibilityRoute, func(w http.ResponseWriter, r *http.Request) {
		if !fullRole(r) {
			apierr.Write(w, http.StatusForbidden, i18n.Err("assistant-tool_visibility.owner_only"))
			return
		}
		var in toolVisibilitySetting
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			apierr.Write(w, http.StatusBadRequest, err)
			return
		}
		if in.ToolVisibility == nil || dec.Decode(new(json.RawMessage)) != io.EOF {
			apierr.Write(w, http.StatusBadRequest, i18n.Err("assistant-tool_visibility.invalid"))
			return
		}
		if err := normalizeToolVisibility(in.ToolVisibility); err != nil {
			apierr.Write(w, http.StatusBadRequest, err)
			return
		}
		if err := s.saveToolVisibility(r.Context(), in.ToolVisibility); err != nil {
			s.logf("mcp visibility: save: %v", err)
			apierr.Write(w, http.StatusServiceUnavailable, i18n.Err("assistant-tool_visibility.unavailable"))
			return
		}
		apierr.WriteJSON(w, in)
	})
}
