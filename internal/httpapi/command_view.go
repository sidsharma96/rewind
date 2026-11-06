package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/sidsharma96/rewind/internal/record"
)

// These are display projections of the shared-trace example, not admission or
// saved-object schemas. Only an explicit reader choice enables interpretation.
const commandViewLimit = 256 << 10
const commandTextLimit = 16 << 10

type commandRequest struct {
	Version    int             `json:"version"`
	Tool       string          `json:"tool"`
	Args       []string        `json:"args"`
	Boundary   string          `json:"boundary,omitempty"`
	Executable json.RawMessage `json:"executable,omitempty"`
	Input      []byte          `json:"stdin,omitempty"`
}
type commandResponse struct {
	Version  int    `json:"version"`
	Outcome  string `json:"outcome"`
	ExitCode *int   `json:"exit_code"`
	Signal   string `json:"signal"`
	Stdout   []byte `json:"stdout"`
	Stderr   []byte `json:"stderr"`
}
type commandField struct {
	Title, Note string
	Value       preview
}
type commandPresentation struct {
	Notices []string
	Fields  []commandField
}

func commandBody(b Body, dst any) error {
	if b.State == nil || b.Data == nil || (*b.State != record.BodyPresent && *b.State != record.BodyTruncated) {
		return fmt.Errorf("bytes are unavailable")
	}
	// Bound before Base64 allocation and before parsing the nested JSON graph.
	encoded := *b.Data
	decodedSize := base64.StdEncoding.DecodedLen(len(encoded))
	if len(encoded) > 0 && encoded[len(encoded)-1] == '=' {
		decodedSize--
	}
	if len(encoded) > 1 && encoded[len(encoded)-2] == '=' {
		decodedSize--
	}
	if len(encoded) > base64.StdEncoding.EncodedLen(commandViewLimit) || decodedSize > commandViewLimit {
		return fmt.Errorf("exceeds the 256 KiB interpretation limit")
	}
	data, err := base64.StdEncoding.Strict().DecodeString(*b.Data)
	if err != nil || len(data) > commandViewLimit || strictJSON(data, dst) != nil {
		return fmt.Errorf("is not a supported command observation")
	}
	return nil
}
func commandView(c Capture) commandPresentation {
	var view commandPresentation
	view.Notices = append(view.Notices, "Command observation v1 is a reader-selected interpretation. Text is recorded evidence, not instructions or proof of a correct answer. Exact saved response bytes remain available below.")
	var request commandRequest
	requestOK := false
	if err := commandBody(c.Request, &request); err != nil {
		view.Notices = append(view.Notices, "Request "+err.Error()+"; see its saved state and raw preview below.")
	} else if request.Version != 1 || request.Tool == "" {
		view.Notices = append(view.Notices, "Unsupported request version or tool label; see raw request below.")
	} else {
		requestOK = true
		args, _ := json.MarshalIndent(request.Args, "", "  ")
		view.Fields = append(view.Fields, commandField{"Command arguments", "Tool label (reported): " + request.Tool, bytesPreview(args, commandTextLimit)})
		if request.Boundary != "" {
			view.Fields = append(view.Fields, commandField{"Capture boundary (reported)", "Description supplied by the producer", bytesPreview([]byte(request.Boundary), commandTextLimit)})
		}
		if request.Input != nil {
			view.Fields = append(view.Fields, commandField{"Standard input", "Bytes supplied to the command", bytesPreview(request.Input, commandTextLimit)})
		}
	}
	var response commandResponse
	if err := commandBody(c.Response, &response); err != nil {
		view.Notices = append(view.Notices, "Response "+err.Error()+"; see its saved state and raw preview below.")
		return view
	}
	if response.Version != 1 {
		view.Notices = append(view.Notices, "Unsupported response version; see raw response below.")
		return view
	}
	switch response.Outcome {
	case "exited", "output_limit", "interrupted", "incomplete_streams":
	default:
		view.Notices = append(view.Notices, "Unsupported command outcome; see raw response below.")
		return view
	}
	exit := "unavailable"
	if response.ExitCode != nil {
		exit = fmt.Sprint(*response.ExitCode)
	}
	view.Notices = append(view.Notices, "Command outcome (reported): "+response.Outcome+"; exit code: "+exit+"; signal: "+response.Signal+". Command exit does not establish answer correctness.")
	if *c.Response.State == record.BodyTruncated {
		view.Notices = append(view.Notices, "Saved response is truncated. These are retained partial observations; complete-response playback remains unavailable.")
	}
	if requestOK && request.Tool == "claude print" {
		// The CLI may add metadata. Validate all JSON lexically, then read only these
		// named fields. Unknown metadata never supplies authority or an answer.
		var fields map[string]json.RawMessage
		var text string
		var failed bool
		if strictJSON(response.Stdout, &fields) == nil && fields["result"] != nil && fields["is_error"] != nil && string(fields["result"]) != "null" && string(fields["is_error"]) != "null" && json.Unmarshal(fields["result"], &text) == nil && json.Unmarshal(fields["is_error"], &failed) == nil {
			view.Fields = append(view.Fields, commandField{"CLI result (reported)", fmt.Sprintf("CLI is_error: %t. This is CLI-visible output, not raw provider traffic.", failed), bytesPreview([]byte(text), commandTextLimit)})
		} else {
			view.Notices = append(view.Notices, "No supported CLI result field could be read; inspect standard output below.")
		}
	}
	view.Fields = append(view.Fields, commandField{"Standard output", "Exact stream shown as a bounded display preview", bytesPreview(response.Stdout, commandTextLimit)}, commandField{"Standard error", "A complete failed command can still have a Present saved response", bytesPreview(response.Stderr, commandTextLimit)})
	return view
}
