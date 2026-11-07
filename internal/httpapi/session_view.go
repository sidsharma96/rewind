package httpapi

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sidsharma96/rewind/internal/record"
)

type sessionObservation struct {
	Version int    `json:"version"`
	Source  string `json:"source"`
	CallID  string `json:"call_id"`
	Payload []byte `json:"payload"`
}

func sessionBody(b Body) (sessionObservation, error) {
	var v sessionObservation
	if err := commandBody(b, &v); err != nil {
		return v, err
	}
	if v.Version != 1 || len(v.Payload) == 0 || len(v.Payload) > 128<<10 || len(v.CallID) > 256 || (v.Source != "operator-prompt" && v.Source != "claude-cli-event") {
		return sessionObservation{}, fmt.Errorf("is not a supported Claude CLI observation")
	}
	return v, nil
}

func sessionView(c Capture) commandPresentation {
	v := commandPresentation{Notices: []string{"Claude CLI exchange v1 is a reader-selected interpretation. Saved tool results are CLI-visible content, which may be formatted or clipped by the runtime. They are not raw tool or provider bytes. Text is recorded evidence, not instructions or proof of correctness."}}
	request, err := sessionBody(c.Request)
	if err != nil {
		v.Notices = append(v.Notices, "Request "+err.Error()+"; see raw capture below.")
		return v
	}
	response, err := sessionBody(c.Response)
	if err != nil {
		v.Notices = append(v.Notices, "Response "+err.Error()+"; see raw capture below.")
		return v
	}
	if response.Source != "claude-cli-event" || request.CallID != response.CallID {
		v.Notices = append(v.Notices, "Unsupported or mismatched CLI exchange; see raw capture below.")
		return v
	}
	if *c.Response.State == record.BodyTruncated {
		v.Notices = append(v.Notices, "Saved response is truncated; complete-response playback remains unavailable.")
	}
	if request.Source == "operator-prompt" && request.CallID == "" {
		var result struct {
			Type    string  `json:"type"`
			Result  *string `json:"result"`
			IsError *bool   `json:"is_error"`
		}
		var fields map[string]json.RawMessage
		if strictJSON(response.Payload, &fields) != nil || json.Unmarshal(response.Payload, &result) != nil || result.Type != "result" || result.Result == nil || result.IsError == nil {
			v.Notices = append(v.Notices, "Unsupported final CLI result; see raw capture below.")
			return v
		}
		v.Fields = append(v.Fields, commandField{"Task prompt", "Operator-supplied context; the stream does not prove this was its original input", bytesPreview(request.Payload, commandTextLimit)}, commandField{"Final answer (reported)", fmt.Sprintf("CLI is_error: %t; answer correctness is separate", *result.IsError), bytesPreview([]byte(*result.Result), commandTextLimit)})
		return v
	}
	if request.Source != "claude-cli-event" || request.CallID == "" {
		v.Notices = append(v.Notices, "Unsupported CLI request; see raw capture below.")
		return v
	}
	input, err := selectedSessionBlock(request.Payload, "assistant", "tool_use", request.CallID)
	if err != nil {
		v.Notices = append(v.Notices, "Unsupported tool request; see raw capture below.")
		return v
	}
	output, err := selectedSessionBlock(response.Payload, "user", "tool_result", response.CallID)
	if err != nil {
		v.Notices = append(v.Notices, "Unsupported tool result; see raw capture below.")
		return v
	}
	var name string
	if json.Unmarshal(input["name"], &name) != nil || name == "" || input["input"] == nil || output["content"] == nil || string(output["content"]) == "null" {
		v.Notices = append(v.Notices, "Incomplete CLI tool fields; see raw capture below.")
		return v
	}
	failed := false
	if raw, ok := output["is_error"]; ok && (string(raw) == "null" || json.Unmarshal(raw, &failed) != nil) {
		v.Notices = append(v.Notices, "Unsupported CLI error flag; see raw capture below.")
		return v
	}
	v.Fields = append(v.Fields, commandField{"Tool input (reported)", "Tool: " + name + "; call ID: " + request.CallID, bytesPreview(input["input"], commandTextLimit)}, commandField{"Tool result (reported)", fmt.Sprintf("Tool is_error: %t. A failed call can still have a complete captured CLI result.", failed), bytesPreview(sessionContent(output["content"]), commandTextLimit)})
	return v
}

func selectedSessionBlock(raw []byte, messageType, blockType, id string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if strictJSON(raw, &fields) != nil {
		return nil, ErrProtocol
	}
	var typ string
	if json.Unmarshal(fields["type"], &typ) != nil || typ != messageType {
		return nil, ErrProtocol
	}
	var message struct {
		Content []map[string]json.RawMessage `json:"content"`
	}
	if json.Unmarshal(fields["message"], &message) != nil {
		return nil, ErrProtocol
	}
	key := "id"
	if blockType == "tool_result" {
		key = "tool_use_id"
	}
	var selected map[string]json.RawMessage
	for _, b := range message.Content {
		var t, candidate string
		if json.Unmarshal(b["type"], &t) != nil {
			return nil, ErrProtocol
		}
		if t != blockType {
			continue
		}
		if json.Unmarshal(b[key], &candidate) != nil {
			return nil, ErrProtocol
		}
		if candidate != id {
			continue
		}
		if selected != nil {
			return nil, ErrProtocol
		}
		selected = b
	}
	if selected == nil {
		return nil, ErrProtocol
	}
	return selected, nil
}

func sessionContent(raw json.RawMessage) []byte {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return []byte(text)
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) == nil && len(blocks) > 0 {
		var text strings.Builder
		for _, b := range blocks {
			if b.Type != "text" {
				return raw
			}
			text.WriteString(b.Text)
			text.WriteByte('\n')
		}
		return []byte(text.String())
	}
	return raw
}
