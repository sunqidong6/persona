package node

import (
	"encoding/json"

	"github.com/nwverse/persona/server/internal/assistantwire"
	"github.com/nwverse/persona/server/internal/wireids"
)

// A watch cursor is not caller identity. Always use the node's current resolution
// and discard the cursor when an initially unknown pane becomes identifiable.
func mcpToolsParams(input json.RawMessage, session string, assistant, guest bool) assistantwire.MCPToolsQuery {
	var cursor struct {
		Revision string `json:"revision"`
		Session  string `json:"session"`
	}
	_ = json.Unmarshal(input, &cursor)
	params := assistantwire.MCPToolsQuery{Assistant: assistant, SessionID: wireids.SessionID(session), Guest: guest, WithVisibility: true}
	if cursor.Session == session {
		params.Revision = cursor.Revision
	}
	return params
}
