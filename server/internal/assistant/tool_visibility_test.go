package assistant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nwverse/persona/server/internal/assistantwire"
	"github.com/nwverse/persona/server/internal/i18n"
	"github.com/nwverse/persona/server/internal/proto"
)

const visibilityRefusedKey = "assistant-tool_visibility.refused"

func visibilityDiscovery(t *testing.T, s *Service, ctx context.Context, input string) assistantwire.MCPToolsReply {
	t.Helper()
	res, handled, err := s.NodeRequest(ctx, "mac", assistantwire.NodeMCPTools, json.RawMessage(input))
	if err != nil || !handled {
		t.Fatalf("discovery handled=%t: %v", handled, err)
	}
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	var out assistantwire.MCPToolsReply
	if err := json.Unmarshal(raw, &out); err != nil || out.Error != "" {
		t.Fatalf("discovery: %s (%v)", raw, err)
	}
	return out
}

func putVisibility(t *testing.T, s *Service, body string, want int) {
	t.Helper()
	w := serveAs(s, roleFull, http.MethodPut, assistantwire.ToolVisibilityRoute, body)
	if w.Code != want {
		t.Fatalf("PUT visibility = %d, want %d: %s", w.Code, want, w.Body)
	}
}

func TestToolVisibilityDiscoveryAndCallsAgree(t *testing.T) {
	ft := newFleetTest(t)
	s, ctx := ft.s, t.Context()
	ft.f.mu.Lock()
	ft.f.hello.Assistant.MachineID, ft.f.hello.Assistant.Machines = "mac", []string{"mac"}
	ft.f.mu.Unlock()
	_, err := s.updateSettings(ctx, func(cfg *Settings) error {
		cfg.Main = "main-chat"
		cfg.BackgroundSupervisor = "background-chat"
		cfg.Leaders = []string{"leader-chat"}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	const ownerTool = "persona_settings"
	const listingTool = "resource_status"
	const list = `{"session_id":"ordinary-chat","with_visibility":true}`
	open := visibilityDiscovery(t, s, ctx, list)
	if len(open.Tools) < 2 || open.ToolVisibility == nil || open.ToolVisibility.Mode != assistantwire.ToolVisibilityOpen {
		t.Fatalf("default discovery %+v", open)
	}
	if pre := visibilityDiscovery(t, s, assistantwire.WithConnectingDiscovery(ctx), list); pre.ToolVisibility.Mode != assistantwire.ToolVisibilityCheck {
		t.Fatalf("an unattached connection received a reusable decision: %+v", pre.ToolVisibility)
	}

	putVisibility(t, s, `{"tool_visibility":{
	    "roles":{
	      "ordinary":{"allowlist":["resource_status","mcp__persona__persona_settings","report_to_supervisor"]},
	      "main":{"denylist":["resource_status"]},
	      "leader":{"denylist":["resource_status"],"expires_at":"2000-01-01T00:00:00Z"},
	      "background":{"allowlist":[]}
	    },
	    "sessions":[{"machine":"mac","session":"ordinary-chat",
	       "denylist":["mcp__jarvis__jarvis_settings","report_to_supervisor"]}]
	}}`, http.StatusOK)

	filtered := visibilityDiscovery(t, s, ctx, list)
	if !slices.Equal(guardNamesOf(filtered.Tools), []string{listingTool}) ||
		filtered.Revision == open.Revision || filtered.ToolVisibility.Allows(ownerTool) {
		t.Fatalf("filtered discovery %+v", filtered)
	}
	if !toolListed(s.ToolsFor(ctx, "mac", "other-chat"), ownerTool) ||
		toolListed(s.ToolsFor(ctx, "mac", "ordinary-chat"), ownerTool) {
		t.Fatal("direct discovery did not apply the same session scope")
	}
	for _, alias := range []string{ownerTool, "jarvis_settings", "mcp__persona__persona_settings", "mcp__jarvis__jarvis_settings", "mcp_jarvis_abcdef01_jarvis_settings"} {
		direct := s.Tool(ctx, ToolCall{MachineID: "mac", SessionID: "ordinary-chat", Name: alias})
		params, _ := json.Marshal(map[string]string{"session_id": "ordinary-chat", "name": alias})
		out, handled, err := s.NodeRequest(ctx, "mac", "mcp.call", params)
		if err != nil || !handled {
			t.Fatalf("call %s handled=%t: %v", alias, handled, err)
		}
		for _, result := range []proto.ToolResult{direct, out.(proto.ToolResult)} {
			if !result.IsError || resultText(result) != i18n.T(s.lang(ctx), visibilityRefusedKey) {
				t.Fatalf("%s was not refused by visibility: %+v", alias, result)
			}
		}
	}
	call := s.Tool(ctx, ToolCall{MachineID: "mac", SessionID: "ordinary-chat", Name: listingTool})
	if call.IsError {
		t.Fatalf("listed tool refused: %+v", call)
	}

	for _, row := range []struct {
		machine, session string
		tool             string
		want             bool
	}{
		{"mac", "main-chat", listingTool, false},
		{"mac", "leader-chat", listingTool, true},
		{"mac", "background-chat", listingTool, false},
		{"other", "main-chat", listingTool, true},
		{"other", "ordinary-chat", ownerTool, true},
		{"mac", "ordinary-chat", "mcp__other__jarvis_settings", false}, // allowlist is exact
	} {
		if got := s.toolVisibilityDecision(ctx, row.machine, row.session, false).Allows(row.tool); got != row.want {
			t.Errorf("%s/%s %s allowed=%t, want %t", row.machine, row.session, row.tool, got, row.want)
		}
	}
	anonymous := visibilityDiscovery(t, s, ctx, `{"with_visibility":true}`)
	if anonymous.ToolVisibility != nil || !toolListed(anonymous.Tools, "report_to_supervisor") {
		t.Fatalf("session-less registration acquired attribution or lost its catalog: %+v", anonymous)
	}
	missing := visibilityDiscovery(t, s, ctx, `{"visibility_only":true}`)
	if missing.ToolVisibility.Mode != assistantwire.ToolVisibilityFailClosed || missing.Tools != nil {
		t.Fatalf("session-less call acquired shared authorization: %+v", missing)
	}
	putVisibility(t, s, `{"tool_visibility":{}}`, http.StatusOK)
	if got := visibilityDiscovery(t, s, ctx, list); got.ToolVisibility.Mode != assistantwire.ToolVisibilityOpen ||
		!slices.Equal(guardNamesOf(got.Tools), guardNamesOf(open.Tools)) {
		t.Fatalf("clearing did not restore default discovery: %+v", got)
	}
}

func TestToolVisibilityExpiryAndExactAliasMatching(t *testing.T) {
	expires := time.Date(2035, 3, 1, 0, 0, 0, 0, time.UTC)
	rule, err := normalizeVisibilityRule(assistantwire.ToolVisibilityRule{
		Denylist: []string{"mcp__jarvis__jarvis_settings", "mcp__persona__ask_persona"}, ExpiresAt: expires,
	})
	if err != nil {
		t.Fatal(err)
	}
	decision := visibilityOpen
	applyVisibilityRule(&decision, rule, expires.Add(-time.Nanosecond))
	for _, name := range []string{"persona_settings", "jarvis_settings", "ask_jarvis", "mcp__jarvis__ask_persona"} {
		if decision.Allows(name) {
			t.Errorf("denied alias %q was allowed", name)
		}
	}
	for _, name := range []string{"my_jarvis_settings", "mcp__jarvis_extra__jarvis_settings", "mcp__other__ask_persona", "jarvis_settings_extra"} {
		if !decision.Allows(name) {
			t.Errorf("near match %q was denied", name)
		}
	}
	decision = visibilityOpen
	applyVisibilityRule(&decision, rule, expires)
	if !decision.Allows("persona_settings") {
		t.Fatal("expiry remained restrictive at its boundary")
	}
}

func TestToolVisibilityPolicyRequiresFullCredentialsAndValidTargets(t *testing.T) {
	s := newFleetTest(t).s
	const path = assistantwire.ToolVisibilityRoute
	for _, role := range []string{roleExtension, "answer"} {
		for _, method := range []string{http.MethodGet, http.MethodPut} {
			w := serveAs(s, role, method, path, `{"tool_visibility":{}}`)
			if w.Code != http.StatusForbidden {
				t.Errorf("%s %s returned %d", role, method, w.Code)
			}
		}
	}
	for _, body := range []string{
		`{}`,
		`{"tool_visibility":{"roles":{"unsupported":{"denylist":["resource_status"]}}}}`,
		`{"tool_visibility":{"roles":{"ordinary":{"allowlist":["*"]}}}}`,
		`{"tool_visibility":{"sessions":[{"machine":"mac","session":"","allowlist":[]}]}}`,
		`{"tool_visibility":{"sessions":[{"machine":"mac","session":"chat"},{"machine":"mac","session":"chat"}]}}`,
	} {
		putVisibility(t, s, body, http.StatusBadRequest)
	}
}

type toolVisibilityTestStore struct {
	*fleetMem
	failRead atomic.Bool
	onPut    func()
}

func (s *toolVisibilityTestStore) Setting(ctx context.Context, key string, out any) (bool, error) {
	if key == toolVisibilityKey && s.failRead.Load() {
		return false, errors.New("settings unavailable")
	}
	return s.fleetMem.Setting(ctx, key, out)
}

func (s *toolVisibilityTestStore) PutSetting(ctx context.Context, key string, value any) error {
	if key == toolVisibilityKey && s.onPut != nil {
		s.onPut()
	}
	return s.fleetMem.PutSetting(ctx, key, value)
}

func TestToolVisibilityColdStartFailsClosedAndRecovers(t *testing.T) {
	st := &toolVisibilityTestStore{fleetMem: newFleetMem()}
	st.failRead.Store(true)
	ft := newFleetTestOn(t, st)
	s, ctx := ft.s, t.Context()
	const query = `{"session_id":"ordinary-chat","with_visibility":true}`
	if res := visibilityDiscovery(t, s, ctx, query); len(res.Tools) != 0 || res.ToolVisibility.Mode != assistantwire.ToolVisibilityFailClosed {
		t.Fatalf("unreadable cold settings returned %+v", res)
	}
	if res := s.Tool(ctx, ToolCall{MachineID: "mac", SessionID: "ordinary-chat", Name: "resource_status"}); !res.IsError {
		t.Fatal("a direct tool call survived a cold settings failure")
	}
	st.failRead.Store(false)
	if res := visibilityDiscovery(t, s, ctx, query); len(res.Tools) == 0 || res.ToolVisibility.Mode != assistantwire.ToolVisibilityOpen {
		t.Fatalf("settings recovery returned %+v", res)
	}
	putVisibility(t, s, `{"tool_visibility":{"roles":{"ordinary":{"denylist":["resource_status"]}}}}`, http.StatusOK)
	st.failRead.Store(true)
	s.refreshToolVisibility(ctx, func() bool { return true }, func() {})
	if res := visibilityDiscovery(t, s, ctx, query); toolListed(res.Tools, "resource_status") || res.ToolVisibility.Mode != assistantwire.ToolVisibilityCheck {
		t.Fatalf("failed refresh discarded the last good policy: %+v", res)
	}
}

func TestToolVisibilityInvalidatesBeforeSavingAndRejectsFailedFanout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	st := &toolVisibilityTestStore{fleetMem: newFleetMem()}
	ft := newFleetTestOn(t, st)
	first := ft.d.machines["mac"]
	first.away = true
	const total = 10
	var active, peak, finished, saved atomic.Int32
	var fail atomic.Bool
	started := make(chan struct{}, total)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	for i := range total {
		n := ft.d.add(fmt.Sprintf("computer-%d", i), "Computer")
		n.call = func(method string, _ json.RawMessage) (any, error) {
			if method != assistantwire.NodeMCPVisibilityInvalidate {
				return struct{}{}, nil
			}
			concurrent := active.Add(1)
			for p := peak.Load(); concurrent > p && !peak.CompareAndSwap(p, concurrent); p = peak.Load() {
			}
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			active.Add(-1)
			finished.Add(1)
			if i == 0 {
				return nil, errors.New("unknown method " + method)
			}
			if i == 1 && fail.Load() {
				return nil, errors.New("node rejected the invalidation")
			}
			return struct{}{}, nil
		}
	}
	st.onPut = func() {
		if active.Load() != 0 || finished.Load() != total {
			t.Error("stored a policy before all connected nodes had replied")
		}
		saved.Add(1)
	}
	s := ft.s
	const query = `{"session_id":"chat","visibility_only":true}`
	if old := visibilityDiscovery(t, s, ctx, query); old.ToolVisibility.Mode != assistantwire.ToolVisibilityOpen {
		t.Fatalf("initial discovery: %+v", old)
	}
	body := `{"tool_visibility":{"roles":{"ordinary":{"denylist":["resource_status"]}}}}`
	status := make(chan int, 1)
	go func() { status <- serve(s, http.MethodPut, assistantwire.ToolVisibilityRoute, body).Code }()
	for range 8 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("invalidation did not fan out concurrently")
		}
	}
	if inFlight := visibilityDiscovery(t, s, ctx, query); inFlight.ToolVisibility.Mode != assistantwire.ToolVisibilityFailClosed {
		t.Fatalf("a node could refill its cache while the policy write was in flight: %+v", inFlight)
	}
	releaseOnce.Do(func() { close(release) })
	if code := <-status; code != http.StatusOK || peak.Load() != 8 || saved.Load() != 1 {
		t.Fatalf("policy write status %d, peak %d, saves %d", code, peak.Load(), saved.Load())
	}
	if calls := ft.d.callsTo("mac", assistantwire.NodeMCPVisibilityInvalidate); len(calls) != 0 {
		t.Fatalf("invalidated an offline node: %v", calls)
	}
	fail.Store(true)
	finished.Store(0)
	for len(started) != 0 {
		<-started
	}
	putVisibility(t, s, `{"tool_visibility":{}}`, http.StatusServiceUnavailable)
	if saved.Load() != 1 || finished.Load() != total || visibilityDiscovery(t, s, ctx, query).ToolVisibility.Allows("resource_status") {
		t.Fatal("failed fanout changed or exposed the policy")
	}
	if read := serve(s, http.MethodGet, assistantwire.ToolVisibilityRoute, ""); read.Code != http.StatusOK ||
		!strings.Contains(read.Body.String(), "resource_status") {
		t.Fatalf("last saved policy unavailable: %s", read.Body)
	}
}

type toolVisibilityExtensionHello struct {
	Extensions
	payloads chan json.RawMessage
}

func (e *toolVisibilityExtensionHello) Hello(raw json.RawMessage)          { e.payloads <- raw }
func (e *toolVisibilityExtensionHello) Event(string, json.RawMessage) bool { return false }

func TestToolVisibilityReconnectOrdersUpdatedAndOmitsExtensionHelloPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ext := &toolVisibilityExtensionHello{payloads: make(chan json.RawMessage, 3)}
	b := NewBusHub(BusOptions{Extensions: ext})
	st := newFleetMem()
	d := newFleetDevs()
	n := d.add("mac", "Mac")
	s := New(Options{Hub: newFakeHub(), Store: func() Store { return st }, Devices: d, Logf: testLogf(t)})
	b.svc, b.ctx = s, ctx
	firstEvents := b.client.OnConnecting()
	if err := b.onHello(json.RawMessage(`{"owner":"first"}`)); err != nil {
		t.Fatal(err)
	}
	<-ext.payloads
	if err := st.PutSetting(ctx, toolVisibilityKey, assistantwire.ToolVisibilityPolicies{
		Roles: map[assistantwire.ToolVisibilityRole]assistantwire.ToolVisibilityRule{
			assistantwire.ToolRoleOrdinary: {Denylist: []string{"resource_status"}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	nextEvents := b.client.OnConnecting()
	var invalidations atomic.Int32
	n.call = func(method string, _ json.RawMessage) (any, error) {
		if method == assistantwire.NodeMCPVisibilityInvalidate {
			i := invalidations.Add(1)
			owner := b.helloNow().Owner
			want := "first"
			if i == 2 {
				want = "second"
			}
			if owner != want || s.toolVisibilityDecision(ctx, "mac", "chat", false).Mode != assistantwire.ToolVisibilityFailClosed {
				t.Errorf("during invalidation %d, owner=%q, want %q with discovery closed", i, owner, want)
			}
		}
		return struct{}{}, nil
	}
	// An event read after the second transport starts belongs to that connection even before its hello is kept.
	nextEvents(assistantwire.EventUpdated, json.RawMessage(`{"owner":"updated","tool_visibility":{"secret":true}}`))
	ev, ok := b.events.take()
	if !ok || ev.conn != 2 {
		t.Fatalf("the update did not retain its connection: %+v", ev)
	}
	update := make(chan error, 1)
	go func() { update <- b.event(ctx, ev) }()
	if err := b.onHello(json.RawMessage(`{"owner":"second","tool_visibility":{"secret":true}}`)); err != nil {
		t.Fatal(err)
	}
	if err := <-update; err != nil {
		t.Fatal(err)
	}
	firstEvents(assistantwire.EventUpdated, json.RawMessage(`{"owner":"stale"}`))
	stale, ok := b.events.take()
	if !ok || stale.conn != 1 {
		t.Fatalf("stale update was stamped as a new connection: %+v", stale)
	}
	if err := b.event(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if b.helloNow().Owner != "updated" || invalidations.Load() != 2 ||
		s.toolVisibilityDecision(ctx, "mac", "chat", false).Allows("resource_status") {
		t.Fatal("reconnect and update did not publish the latest hello and policy in order")
	}
	for _, owner := range []string{"second", "updated"} {
		select {
		case raw := <-ext.payloads:
			if strings.Contains(strings.ReplaceAll(strings.ToLower(string(raw)), "_", ""), "toolvisibility") ||
				strings.Contains(string(raw), "resource_status") {
				t.Fatalf("extension hello carried visibility: %s", raw)
			}
			var hello assistantwire.Hello
			if err := json.Unmarshal(raw, &hello); err != nil || hello.Owner != owner {
				t.Fatalf("extension hello owner=%q, want %q (%v)", hello.Owner, owner, err)
			}
		case <-ctx.Done():
			t.Fatal("extension hello was not delivered")
		}
	}
}
