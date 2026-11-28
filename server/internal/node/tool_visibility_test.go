package node

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nwverse/persona/server/internal/assistantwire"
	"github.com/nwverse/persona/server/internal/proto"
)

func TestToolVisibilityRuntimeDiscoveryAndHooks(t *testing.T) {
	n, hub := runtimeNode(t)
	assistantHub(hub)
	mode := atomic.Value{}
	mode.Store(assistantwire.ToolVisibilityOpen)
	var mu sync.Mutex
	var requests []assistantwire.MCPToolsQuery
	hub.mu.Lock()
	answer := hub.answer
	hub.answer = func(method string, raw json.RawMessage) (any, error, bool) {
		if method != assistantwire.NodeMCPTools {
			return answer(method, raw)
		}
		var p assistantwire.MCPToolsQuery
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err, true
		}
		mu.Lock()
		requests = append(requests, p)
		mu.Unlock()
		decision := assistantwire.ToolVisibilityDecision{Mode: mode.Load().(assistantwire.ToolVisibilityMode)}
		if decision.Mode == assistantwire.ToolVisibilityCheck {
			decision.Denylist = []string{"example_api"}
		}
		reply := assistantwire.MCPToolsReply{}
		if p.SessionID != "" || decision.Mode == assistantwire.ToolVisibilityFailClosed {
			reply.ToolVisibility = &decision
		}
		if !p.VisibilityOnly {
			reply.Tools = decision.Filter([]proto.Tool{{Name: "phone_contacts", InputSchema: json.RawMessage(`{"type":"object"}`)}})
		}
		return reply, nil, true
	}
	hub.mu.Unlock()

	skills := t.TempDir()
	t.Setenv("PERSONA_SKILLS_DIR", skills)
	t.Setenv("PERSONA_PLUGIN_SKILLS_DIR", t.TempDir())
	n.dir = filepath.Join(n.home, ".persona")
	script := filepath.Join(skills, "example", "scripts", "ex.sh")
	if err := os.MkdirAll(filepath.Dir(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skills, "example", "SKILL.md"), []byte("---\nname: example\n---\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(script, []byte("#!/bin/bash\necho ran \"$*\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	prepareAssistantForTest(t, n, ".persona/assistant", nil, nil)
	gate := exampleGate("")
	gate.Tool = &proto.GateTool{Name: "example_api", Description: "Run the example."}
	n.mu.Lock()
	n.larkCfg, n.larkCfgAt = proto.AssistantLark{Gates: []proto.CommandGate{gate}}, time.Now().Add(time.Hour)
	n.mu.Unlock()
	host := dialRuntime(t, n, assistantRuntimeHello())
	sid := startedAssistant(t, n, host)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	list := func(session string) []proto.Tool {
		t.Helper()
		var result assistantwire.MCPToolsReply
		if err := host.peer.Call(ctx, assistantwire.NodeMCPTools, map[string]any{"assistant": true, "session_id": session}, &result); err != nil {
			t.Fatal(err)
		}
		if result.ToolVisibility != nil {
			t.Fatal("manager visibility metadata leaked to the runtime")
		}
		return result.Tools
	}
	call := func() proto.ToolResult {
		t.Helper()
		var result proto.ToolResult
		if err := host.peer.Call(ctx, "mcp.call", map[string]any{
			"name": "mcp__jarvis__example_api", "args": map[string]any{"args": []string{"GET", "/me"}},
			"session_id": sid, "assistant": true,
		}, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	requestSnapshot := func() []assistantwire.MCPToolsQuery {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(requests)
	}
	if got := toolList(list("")); !strings.Contains(got, "example_api") {
		t.Fatalf("legacy session-less registration lost its local catalog: %q", got)
	}
	for range 2 {
		if result := call(); result.IsError || !strings.Contains(result.Content[0].Text, "ran GET /me") {
			t.Fatalf("default-open local gate call failed: %+v", result)
		}
	}
	initial := requestSnapshot()
	if len(initial) != 2 || initial[0].SessionID != "" || initial[0].VisibilityOnly ||
		initial[1].SessionID.String() != sid || !initial[1].VisibilityOnly {
		t.Fatalf("expected only the initial attributed local check: %+v", initial)
	}
	list(sid)
	if result := call(); result.IsError || len(requestSnapshot()) != 3 {
		t.Fatalf("an ordinary attributed discovery added a per-call RPC: %+v / %+v", result, requestSnapshot())
	}

	invalidate := func() {
		t.Helper()
		if err := hub.nodePeer.Call(ctx, assistantwire.NodeMCPVisibilityInvalidate, struct{}{}, nil); err != nil {
			t.Fatal(err)
		}
	}
	mode.Store(assistantwire.ToolVisibilityFailClosed)
	invalidate()
	if result := call(); !result.IsError || result.Content[0].Text != toolVisibilityDenied().Content[0].Text {
		t.Fatalf("a fail-closed decision admitted the gate: %+v", result)
	}
	mode.Store(assistantwire.ToolVisibilityOpen)
	if result := call(); result.IsError {
		t.Fatalf("the node did not recover its cached FailClosed decision: %+v", result)
	}
	mode.Store(assistantwire.ToolVisibilityCheck)
	invalidate()
	if got := toolList(list(sid)); strings.Contains(got, "example_api") {
		t.Fatalf("denied local gate advertised by runtime: %q", got)
	}
	if result := call(); !result.IsError {
		t.Fatalf("denied runtime gate ran: %+v", result)
	}
	input, _ := json.Marshal(map[string]string{"session_id": sid, "hook_event_name": "PreToolUse", "tool_name": "mcp__jarvis__example_api"})
	out, err := n.handleHook(ctx, sockRequest{Kind: "hook", Input: input})
	raw, _ := json.Marshal(out)
	if err != nil || !strings.Contains(string(raw), `"permissionDecision":"deny"`) {
		t.Fatalf("the MCP hook allowed a denied alias: %s (%v)", raw, err)
	}
}

func TestToolVisibilityInvalidationRejectsInFlightDiscovery(t *testing.T) {
	n, hub := runtimeNode(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	hub.mu.Lock()
	hub.answer = func(method string, raw json.RawMessage) (any, error, bool) {
		if method != assistantwire.NodeMCPTools {
			return nil, nil, false
		}
		var query assistantwire.MCPToolsQuery
		if err := json.Unmarshal(raw, &query); err != nil {
			return nil, err, true
		}
		if query.Revision == "old" {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return assistantwire.MCPToolsReply{ToolVisibility: &assistantwire.ToolVisibilityDecision{Mode: assistantwire.ToolVisibilityOpen}}, nil, true
		}
		return assistantwire.MCPToolsReply{ToolVisibility: &assistantwire.ToolVisibilityDecision{
			Mode: assistantwire.ToolVisibilityCheck, Denylist: []string{"example_api"},
		}}, nil, true
	}
	hub.mu.Unlock()
	old := make(chan error, 1)
	go func() {
		_, err := n.discoverMCP(ctx, assistantwire.MCPToolsQuery{SessionID: "session", Revision: "old", WithVisibility: true})
		old <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("discovery did not start")
	}
	if err := hub.nodePeer.Call(ctx, assistantwire.NodeMCPVisibilityInvalidate, nil, nil); err != nil {
		t.Fatal(err)
	}
	if n.localToolVisible(ctx, "session", "example_api") {
		t.Fatal("missing decision allowed a call after invalidation")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-old; err == nil {
		t.Fatal("accepted an earlier discovery response")
	}
	if n.localToolVisible(ctx, "session", "example_api") {
		t.Fatal("stale discovery restored an already revoked grant")
	}
}

func TestToolVisibilityResumeDiscardsOpenDecisions(t *testing.T) {
	var requests atomic.Int32
	var restrict atomic.Bool
	r := startResidentAttachWithRequests(t, func(_ context.Context, method string, _ json.RawMessage) (any, error) {
		if method != assistantwire.NodeMCPTools {
			return struct{}{}, nil
		}
		requests.Add(1)
		mode := assistantwire.ToolVisibilityOpen
		if restrict.Load() {
			mode = assistantwire.ToolVisibilityFailClosed
		}
		return assistantwire.MCPToolsReply{ToolVisibility: &assistantwire.ToolVisibilityDecision{Mode: mode}}, nil
	})
	defer r.end()
	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()
	n := r.node.Load()
	for range 2 {
		if !n.localToolVisible(ctx, "session", "example_api") {
			t.Fatal("initial local tool was blocked")
		}
	}
	if count := requests.Load(); count != 1 {
		t.Fatalf("initial local calls sent %d visibility requests", count)
	}
	r.first.Close()
	select {
	case <-r.lost:
	case <-ctx.Done():
		t.Fatal("transport loss not observed")
	}
	restrict.Store(true) // changed while the manager had no live transport to invalidate
	a, b := net.Pipe()
	defer a.Close()
	r.extra <- b
	if err := r.hub.Resume(ctx, a, a); err != nil {
		t.Fatal(err)
	}
	if n.localToolVisible(ctx, "session", "example_api") || requests.Load() != 2 || r.made.Load() != 1 {
		t.Fatalf("resume reused old authorization, requests=%d nodes=%d", requests.Load(), r.made.Load())
	}
}
