package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/winghv/agentwharf/protocol"
)

const (
	piAgentDirEnv      = "PI_CODING_AGENT_DIR"
	maxPiSettingsBytes = 1 << 20
)

var piThinkingLevels = map[string]struct{}{
	"off": {}, "minimal": {}, "low": {}, "medium": {}, "high": {}, "xhigh": {}, "max": {},
}

type piACPModelChoice struct {
	id      string
	modelID string
	name    string
}

// Pi's /scoped-models selector persists its selection in user-level settings.
func piEnabledModelPatterns() []string {
	agentDir := strings.TrimSpace(os.Getenv(piAgentDirEnv))
	if agentDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil
		}
		agentDir = filepath.Join(home, ".pi", "agent")
	} else if agentDir == "~" || strings.HasPrefix(agentDir, "~/") || strings.HasPrefix(agentDir, "~\\") {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil
		}
		if agentDir == "~" {
			agentDir = home
		} else {
			agentDir = filepath.Join(home, agentDir[2:])
		}
	}
	return readPiEnabledModelPatterns(filepath.Join(agentDir, "settings.json"))
}

func readPiEnabledModelPatterns(settingsPath string) []string {
	file, err := os.Open(settingsPath)
	if err != nil {
		return nil
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxPiSettingsBytes+1))
	if err != nil || len(data) > maxPiSettingsBytes {
		return nil
	}
	data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
	var settings struct {
		EnabledModels json.RawMessage `json:"enabledModels"`
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		return nil
	}
	var values []json.RawMessage
	if len(settings.EnabledModels) == 0 || json.Unmarshal(settings.EnabledModels, &values) != nil {
		return nil
	}
	patterns := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		var pattern string
		if err := json.Unmarshal(value, &pattern); err != nil {
			continue
		}
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		if _, ok := seen[pattern]; ok {
			continue
		}
		seen[pattern] = struct{}{}
		patterns = append(patterns, pattern)
	}
	return patterns
}

// Pi ACP advertises the available catalogue; the CLI scope must be reapplied here.
func filterPiACPModelChoices(
	choices map[string]protocol.SettingsCapabilityChoice,
	currentID string,
	patterns []string,
) (map[string]protocol.SettingsCapabilityChoice, bool) {
	if len(patterns) == 0 {
		return nil, false
	}
	models := make([]piACPModelChoice, 0, len(choices))
	for id, choice := range choices {
		provider, modelID := splitPiACPModelID(id)
		name := choice.Label
		prefix := provider + "/"
		if provider != "" && strings.HasPrefix(strings.ToLower(name), strings.ToLower(prefix)) {
			name = name[len(prefix):]
		}
		models = append(models, piACPModelChoice{id: id, modelID: modelID, name: name})
	}

	matched := make(map[string]struct{}, len(patterns))
	for _, pattern := range patterns {
		for _, id := range resolvePiACPModelPattern(pattern, models) {
			matched[id] = struct{}{}
		}
	}
	if len(matched) == 0 {
		return nil, false
	}
	if _, ok := choices[currentID]; ok {
		matched[currentID] = struct{}{}
	}
	filtered := make(map[string]protocol.SettingsCapabilityChoice, len(matched))
	for id := range matched {
		if choice, ok := choices[id]; ok {
			filtered[id] = choice
		}
	}
	return filtered, true
}

func splitPiACPModelID(id string) (provider, modelID string) {
	separator := strings.IndexByte(id, '/')
	if separator <= 0 || separator == len(id)-1 {
		return "", id
	}
	return id[:separator], id[separator+1:]
}

func resolvePiACPModelPattern(pattern string, models []piACPModelChoice) []string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return nil
	}
	lowerPattern := strings.ToLower(pattern)
	if strings.ContainsAny(pattern, "*?[") {
		globPattern := pattern
		if prefix, ok := stripPiThinkingSuffix(pattern); ok {
			globPattern = prefix
		}
		if exact := exactPiACPModelChoice(globPattern, models); exact != "" {
			return []string{exact}
		}
		lowerGlob := strings.ToLower(globPattern)
		var matched []string
		for _, model := range models {
			if piGlobMatch(lowerGlob, strings.ToLower(model.id)) || piGlobMatch(lowerGlob, strings.ToLower(model.modelID)) {
				matched = append(matched, model.id)
			}
		}
		return matched
	}

	if match := bestPiACPModelMatch(lowerPattern, models); match != "" {
		return []string{match}
	}
	if index := strings.LastIndex(pattern, ":"); index >= 0 {
		return resolvePiACPModelPattern(pattern[:index], models)
	}
	return nil
}

func exactPiACPModelChoice(pattern string, models []piACPModelChoice) string {
	lower := strings.ToLower(strings.TrimSpace(pattern))
	var exact []piACPModelChoice
	for _, model := range models {
		if strings.ToLower(model.id) == lower {
			return model.id
		}
		if strings.ToLower(model.modelID) == lower {
			exact = append(exact, model)
		}
	}
	if len(exact) == 1 {
		return exact[0].id
	}
	return ""
}

func bestPiACPModelMatch(pattern string, models []piACPModelChoice) string {
	if exact := exactPiACPModelChoice(pattern, models); exact != "" {
		return exact
	}
	var matches []piACPModelChoice
	for _, model := range models {
		if strings.Contains(strings.ToLower(model.modelID), pattern) || strings.Contains(strings.ToLower(model.name), pattern) {
			matches = append(matches, model)
		}
	}
	if len(matches) == 0 {
		return ""
	}
	sort.Slice(matches, func(i, j int) bool {
		iAlias, jAlias := !piModelIDHasDate(matches[i].modelID), !piModelIDHasDate(matches[j].modelID)
		if iAlias != jAlias {
			return iAlias
		}
		if matches[i].modelID != matches[j].modelID {
			return matches[i].modelID > matches[j].modelID
		}
		return matches[i].id > matches[j].id
	})
	return matches[0].id
}

func piModelIDHasDate(id string) bool {
	separator := strings.LastIndexByte(id, '-')
	if separator < 0 || len(id)-separator != 9 {
		return false
	}
	for _, digit := range id[separator+1:] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}

func piGlobMatch(pattern, value string) bool {
	matched, err := path.Match(pattern, value)
	return err == nil && matched
}

func stripPiThinkingSuffix(pattern string) (string, bool) {
	index := strings.LastIndexByte(pattern, ':')
	if index < 0 {
		return pattern, false
	}
	if _, ok := piThinkingLevels[strings.ToLower(pattern[index+1:])]; !ok {
		return pattern, false
	}
	return pattern[:index], true
}
