package protocol

import (
	"encoding/json"
	"errors"
	"strings"
)

// EncryptedLaunchSettings is endpoint-only configuration carried inside a
// signed session.send payload, never as relay-visible launch metadata.
// WorkingDirectoryID is resolved only by the local AgentWharf registry. The
// platform and Hub carry the launch payload as endpoint-encrypted content.
type EncryptedLaunchSettings struct {
	Provider           string `json:"provider,omitempty"`
	WorkingDirectory   string `json:"working_directory,omitempty"`
	WorkingDirectoryID string `json:"working_directory_id,omitempty"`
	ModelID            string `json:"model_id,omitempty"`
	ReasoningEffortID  string `json:"reasoning_effort_id,omitempty"`
	PermissionModeID   string `json:"permission_mode_id,omitempty"`
}

func DecodeEncryptedLaunchSettings(payload json.RawMessage) (EncryptedLaunchSettings, error) {
	var result EncryptedLaunchSettings
	fields, err := strictObject(payload)
	if err != nil {
		return result, errors.New("invalid launch payload")
	}
	raw, found := fields["launch"]
	if !found {
		return result, nil
	}
	launch, err := strictObject(raw)
	if err != nil {
		return result, errors.New("invalid encrypted launch settings")
	}
	for name, value := range launch {
		var text string
		if string(value) == "null" || json.Unmarshal(value, &text) != nil {
			return result, errors.New("invalid encrypted launch setting")
		}
		switch name {
		case "provider":
			if !validSettingsIdentifier(text) {
				return result, errors.New("invalid encrypted launch provider")
			}
			result.Provider = text
		case "working_directory":
			if len(text) > 4096 || strings.ContainsAny(text, "\x00\r\n") {
				return result, errors.New("invalid encrypted working directory")
			}
			result.WorkingDirectory = text
		case "working_directory_id":
			if !validDirectoryIdentityID(text) {
				return result, errors.New("invalid encrypted working directory identity")
			}
			result.WorkingDirectoryID = text
		case "model_id", "reasoning_effort_id", "permission_mode_id":
			if !validSettingsIdentifier(text) {
				return result, errors.New("invalid encrypted launch identifier")
			}
			switch name {
			case "model_id":
				result.ModelID = text
			case "reasoning_effort_id":
				result.ReasoningEffortID = text
			case "permission_mode_id":
				result.PermissionModeID = text
			}
		default:
			return result, errors.New("unknown encrypted launch setting")
		}
	}
	if result.WorkingDirectory != "" && result.WorkingDirectoryID != "" {
		return result, errors.New("encrypted working directory and identity are mutually exclusive")
	}
	return result, nil
}

func validDirectoryIdentityID(value string) bool {
	const prefix = "dir_v1_"
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+22 {
		return false
	}
	for _, character := range strings.TrimPrefix(value, prefix) {
		if (character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') &&
			(character < '0' || character > '9') && character != '_' && character != '-' {
			return false
		}
	}
	return true
}
