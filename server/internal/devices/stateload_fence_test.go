package devices

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nwverse/persona/server/internal/db"
	"github.com/nwverse/persona/server/internal/proto"
	"github.com/nwverse/persona/server/internal/rpc"
)

// heldStateStore keeps what PutNodeSession wrote, but holds every write until the test lets one (one) or all (let)
// through, so a test can show what a state.load answers while the write before it is still on its way.
type heldStateStore struct {
	*memStore

	entered     chan string
	one         chan struct{}
	release     chan struct{}
	releaseOnce sync.Once

	mu       sync.Mutex
	sessions map[string]db.NodeSession
}

// let lets every held and later write through.
func (s *heldStateStore) let() { s.releaseOnce.Do(func() { close(s.release) }) }

func newHeldStateStore() *heldStateStore {
	return &heldStateStore{memStore: newMemStore(), entered: make(chan string, 64), one: make(chan struct{}),
		release: make(chan struct{}), sessions: map[string]db.NodeSession{}}
}

func (s *heldStateStore) PutNodeSession(_ context.Context, _, session string, state db.NodeSession) error {
	select {
	case s.entered <- session:
	default:
	}
	select {
	case <-s.one:
	case <-s.release:
	}
	s.mu.Lock()
	s.sessions[session] = state
	s.mu.Unlock()
	return nil
}

func (s *heldStateStore) NodeSessions(context.Context, string) (map[string]db.NodeSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]db.NodeSession, len(s.sessions))
	for id, st := range s.sessions {
		out[id] = st
	}
	return out, nil
}

// stateLoadWorld is a serving Machine and the node end of its connection, wired the way conn.go wires a node:
// events through eventRetire on the read loop, requests on their own goroutine.
type stateLoadWorld struct {
	m     *Machine
	st    *heldStateStore
	node  *rpc.Peer
	ctx   context.Context
	loads chan struct{} // a state.load reached the Machine: every event sent before it was admitted
}

func newStateLoadWorld(t *testing.T, storeWait time.Duration) *stateLoadWorld {
	t.Helper()
	st := newHeldStateStore()
	d := New(Options{Hub: &fakeHost{}, Store: st, Logf: func(string, ...any) {}})
	m := newMachine(d, MachineConfig{ID: "box", Name: "Box", Kind: "ssh", SSHHost: "box.invalid"})
	m.storeWait = storeWait
	m.ctx, m.stop = context.WithCancel(context.Background())
	hubConn, nodeConn := net.Pipe()
	hubPeer := rpc.New("hub", hubConn)
	nodePeer := rpc.New("node", nodeConn)
	hubPeer.OnEvent = func(event string, params json.RawMessage) {
		m.eventRetire.Lock()
		defer m.eventRetire.Unlock()
		m.onEvent(event, params)
	}
	loads := make(chan struct{}, 16)
	hubPeer.OnRequest = func(ctx context.Context, method string, params json.RawMessage) (any, error) {
		if method == "state.load" {
			loads <- struct{}{}
		}
		return m.onRequest(ctx, method, params)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go hubPeer.Serve(ctx, hubConn)
	go nodePeer.Serve(ctx, nodeConn)
	t.Cleanup(func() {
		st.let()
		cancel()
		m.stop()
		if w := m.stateWriter; w != nil && !w.waitDone(5*time.Second) {
			t.Error("node-state writer did not stop")
		}
	})
	return &stateLoadWorld{m: m, st: st, node: nodePeer, ctx: ctx, loads: loads}
}

func (w *stateLoadWorld) put(t *testing.T, session string, state map[string]any) {
	t.Helper()
	if err := w.node.Notify("state.put", map[string]any{"session_id": session, "state": state}); err != nil {
		t.Fatalf("state.put: %v", err)
	}
	select {
	case got := <-w.st.entered:
		if got != session {
			t.Fatalf("PutNodeSession for %q, want %q", got, session)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("state.put never reached PutNodeSession")
	}
}

type loadResult struct {
	sessions map[string]db.NodeSession
	err      error
	took     time.Duration
}

// load sends the node's state.load and returns its answer on a channel.
func (w *stateLoadWorld) load() <-chan loadResult {
	out := make(chan loadResult, 1)
	go func() {
		start := time.Now()
		var res struct {
			Sessions map[string]db.NodeSession `json:"sessions"`
		}
		err := w.node.Call(w.ctx, "state.load", map[string]any{}, &res)
		out <- loadResult{sessions: res.Sessions, err: err, took: time.Since(start)}
	}()
	return out
}

// loadAfterRelease shows a state.load is still waiting for the write before it, then lets that write through and
// returns the load's answer. On the code before #17974 the load answered at once from the Store, without the state.
func (w *stateLoadWorld) loadAfterRelease(t *testing.T, session string) map[string]db.NodeSession {
	t.Helper()
	res := w.load()
	select {
	case r := <-res:
		t.Fatalf("state.load answered while the state.put sent ahead of it was still being written (err %v, sessions %v)", r.err, r.sessions)
	case <-time.After(200 * time.Millisecond):
	}
	w.st.let()
	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("state.load: %v", r.err)
		}
		return r.sessions
	case <-time.After(5 * time.Second):
		t.Fatal("state.load did not answer once the write before it was in the Store")
	}
	return nil
}

// #17974: a state.load reads a state.put the node sent before it on the same connection, as it did before #17300
// moved the write onto the writer goroutine. The share creation receipt relies on it
// (hub TestShareCreationAuthenticatedHTTPReceiptIntegration).
func TestStateLoadReadsTheStatePutBeforeIt(t *testing.T) {
	w := newStateLoadWorld(t, time.Minute) // the held write, not the budget, decides when the load answers
	w.put(t, "s1", map[string]any{"launcher": "claude", "cwd": "/w", "creation_id": "c-1", "incarnation": "i-1"})
	got, ok := w.loadAfterRelease(t, "s1")["s1"]
	if !ok || got.Cwd != "/w" || got.CreationID != "c-1" || got.Incarnation != "i-1" {
		t.Fatalf("state.load = %+v (present %v), want the state put before it", got, ok)
	}
}

// #17974: the node's stop check (saveStopCause, internal/node/stop_provenance.go) puts the stop cause and reads it
// back with state.load; a load that misses it refuses the stop with stop_not_saved. The read-back must see it.
func TestStateLoadReadsBackAStopCause(t *testing.T) {
	w := newStateLoadWorld(t, time.Minute)
	at := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC).UnixNano()
	w.put(t, "s1", map[string]any{"launcher": "claude", "cwd": "/w", "stop_cause": string(proto.StopUser), "stop_cause_at": at})
	got, ok := w.loadAfterRelease(t, "s1")["s1"]
	// The node's own comparison: present, the same cause and the same time.
	if !ok || got.StopCause != proto.StopUser || got.StopCauseAt != at {
		t.Fatalf("stop read-back = %+v (present %v), want cause %q at %d", got, ok, proto.StopUser, at)
	}
}

// #17974: a Store that never finishes the write must not hold a state.load past one Store call budget: the load then
// answers with what the Store has, as a stalled inline write did when it timed out.
func TestStateLoadAnswersWithinTheBudgetWhenTheWriteStalls(t *testing.T) {
	const budget = 300 * time.Millisecond
	w := newStateLoadWorld(t, budget)
	w.put(t, "s1", map[string]any{"launcher": "claude", "cwd": "/w"}) // held until the test ends
	select {
	case r := <-w.load():
		if r.err != nil {
			t.Fatalf("state.load: %v", r.err)
		}
		if _, ok := r.sessions["s1"]; ok {
			t.Fatalf("state.load returned a state the Store never wrote: %v", r.sessions)
		}
		if r.took < budget {
			t.Fatalf("state.load answered after %v, before the %v budget, while the write before it was held", r.took, budget)
		}
	case <-time.After(budget + 2*time.Second):
		t.Fatalf("state.load still waiting %v after a %v budget: a stalled write holds it", budget+2*time.Second, budget)
	}
}

// #17974: a newer state of the same session that coalesces while the older one is being written is still a state the
// load must wait for: once the older write is in the Store and the newer one is still being written, the load keeps
// waiting, then reads the newest, with the creation receipt the older one carried.
func TestStateLoadReadsTheNewestStateOfASessionWrittenTwice(t *testing.T) {
	w := newStateLoadWorld(t, time.Minute)
	w.put(t, "s1", map[string]any{"launcher": "claude", "cwd": "/w1", "creation_id": "c-1", "incarnation": "i-1"})
	// The first write is held in PutNodeSession; this one coalesces behind it.
	if err := w.node.Notify("state.put", map[string]any{"session_id": "s1", "state": map[string]any{"launcher": "claude", "cwd": "/w2"}}); err != nil {
		t.Fatalf("state.put: %v", err)
	}
	res := w.load()
	select {
	case <-w.loads: // the newer state was admitted (and coalesced) before the load
	case <-time.After(5 * time.Second):
		t.Fatal("state.load never reached the Machine")
	}
	select {
	case w.st.one <- struct{}{}: // the first write goes through
	case <-time.After(5 * time.Second):
		t.Fatal("the first write never took its turn")
	}
	select {
	case got := <-w.st.entered: // the newer state is now being written, and held
		if got != "s1" {
			t.Fatalf("PutNodeSession for %q, want s1", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the newer state was never written")
	}
	select {
	case r := <-res:
		t.Fatalf("state.load answered between the two writes of s1 (err %v, sessions %v)", r.err, r.sessions)
	case <-time.After(200 * time.Millisecond):
	}
	w.st.let()
	select {
	case r := <-res:
		got, ok := r.sessions["s1"]
		if r.err != nil || !ok || got.Cwd != "/w2" || got.CreationID != "c-1" || got.Incarnation != "i-1" {
			t.Fatalf("state.load = %+v (present %v, err %v), want the newer state with the older receipt", got, ok, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("state.load did not answer once both writes were in the Store")
	}
}
