package protocol_test

import (
	"encoding/json"
	"github.com/winghv/agentwharf/protocol"
	"testing"
)

func TestEncryptedLaunchSettingsStrictPayload(t *testing.T) {
	settings, err := protocol.DecodeEncryptedLaunchSettings(json.RawMessage(`{"content":[],"launch":{"working_directory":"/workspace/private","model_id":"model","reasoning_effort_id":"high","permission_mode_id":"ask"}}`))
	if err != nil || settings.WorkingDirectory != "/workspace/private" || settings.ModelID != "model" || settings.ReasoningEffortID != "high" || settings.PermissionModeID != "ask" {
		t.Fatalf("launch decode: %+v %v", settings, err)
	}
	for _, payload := range []string{
		`{"launch":null}`, `{"launch":{"model_id":"a","model_id":"b"}}`,
		`{"launch":{},"launch":{}}`, `{"launch":{"env":{"TOKEN":"private"}}}`,
		`{"launch":{"working_directory":"/workspace\u0000"}}`, `{"launch":{"model_id":null}}`,
	} {
		if _, err := protocol.DecodeEncryptedLaunchSettings(json.RawMessage(payload)); err == nil {
			t.Fatal("ambiguous or unsupported launch accepted")
		}
	}
	if settings, err := protocol.DecodeEncryptedLaunchSettings(json.RawMessage(`{"content":[]}`)); err != nil || settings != (protocol.EncryptedLaunchSettings{}) {
		t.Fatal("instruction-only compatibility", err)
	}
}
