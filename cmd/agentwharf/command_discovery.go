package main

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/winghv/agentwharf/adapter/core"
	"github.com/winghv/agentwharf/protocol"
)

const (
	maxDiscoveredCommands       = 64
	maxDiscoveredCommandBytes   = 32 * 1024
	maxDiscoveredNameRunes      = 80
	maxDiscoveredDescription    = 200
	maxDiscoveredDirectoryDepth = 4
)

// discoveredCommand is the only shape that crosses the Adapter boundary: a
// name and description, never the source path or the file body. A client can
// offer the command without learning the project's directory layout.
type discoveredCommand struct {
	Name         string
	Description  string
	Source       string
	ArgumentHint string
}

// discoverProviderCommands reads project-local provider commands and skills.
//
// It intentionally does not read the user's home directory. A shared machine
// can host several tenants, and a global command name is still information
// about another project. Cloud machines and Own Machine projects both provide
// a working directory, which is the boundary this discovery respects.
func discoverProviderCommands(provider, workingDirectory string) []discoveredCommand {
	if workingDirectory == "" {
		workingDirectory, _ = os.Getwd()
	}
	if workingDirectory == "" {
		return nil
	}
	root, err := filepath.Abs(workingDirectory)
	if err != nil {
		return nil
	}
	// Resolve the working directory once and reject any discovery root that
	// escapes it through a symlinked .claude/.codex directory.
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil
	}
	commands := make([]discoveredCommand, 0, 16)
	seen := map[string]struct{}{}
	add := func(command discoveredCommand) {
		if command.Name == "" || len(commands) >= maxDiscoveredCommands {
			return
		}
		key := strings.ToLower(command.Name)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		commands = append(commands, command)
	}

	switch provider {
	case "claude", "claude-code", "":
		discoverClaudeCommands(root, add)
		discoverClaudeSkills(root, add)
	case "codex":
		discoverCommandDirectory(filepath.Join(root, ".codex", "prompts"), root, "", "project", add)
	default:
		discoverClaudeCommands(root, add)
		discoverClaudeSkills(root, add)
	}
	sort.SliceStable(commands, func(i, j int) bool { return commands[i].Name < commands[j].Name })
	return commands
}

func discoverClaudeCommands(root string, add func(discoveredCommand)) {
	discoverCommandDirectory(filepath.Join(root, ".claude", "commands"), root, "", "project", add)
}

func discoverClaudeSkills(root string, add func(discoveredCommand)) {
	skillsRoot := filepath.Join(root, ".claude", "skills")
	resolved, err := filepath.EvalSymlinks(skillsRoot)
	if err != nil || !withinDiscoveryRoot(root, resolved) {
		return
	}
	entries, err := os.ReadDir(resolved)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		path := filepath.Join(resolved, name, "SKILL.md")
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		command, ok := readDiscoveredCommand(path, name, "skill")
		if ok {
			add(command)
		}
	}
}

func discoverCommandDirectory(root, boundary, prefix, source string, add func(discoveredCommand)) {
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || !withinDiscoveryRoot(boundary, resolved) {
		return
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return
	}
	root = resolved
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || relative == "." {
			return nil
		}
		if countPathSegments(relative) > maxDiscoveredDirectoryDepth {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(path), ".md") {
			return nil
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > maxDiscoveredCommandBytes {
			return nil
		}
		name := strings.TrimSuffix(relative, filepath.Ext(relative))
		name = strings.ReplaceAll(name, string(filepath.Separator), ":")
		if prefix != "" {
			name = prefix + ":" + name
		}
		if command, ok := readDiscoveredCommand(path, name, source); ok {
			add(command)
		}
		return nil
	})
}

func readDiscoveredCommand(path, fallbackName, source string) (discoveredCommand, bool) {
	content, err := os.ReadFile(path)
	if err != nil || len(content) > maxDiscoveredCommandBytes {
		return discoveredCommand{}, false
	}
	text := strings.ReplaceAll(string(content), "\r\n", "\n")
	frontmatter, body := splitFrontmatter(text)
	name := strings.TrimSpace(frontmatter["name"])
	if name == "" {
		name = fallbackName
	}
	name = sanitizeDiscoveredText(name, maxDiscoveredNameRunes)
	description := strings.TrimSpace(frontmatter["description"])
	if description == "" {
		description = firstDescriptionLine(body)
	}
	description = sanitizeDiscoveredText(description, maxDiscoveredDescription)
	argumentHint := strings.TrimSpace(frontmatter["argument-hint"])
	if argumentHint == "" {
		argumentHint = strings.TrimSpace(frontmatter["argument_hint"])
	}
	argumentHint = sanitizeDiscoveredText(argumentHint, 120)
	if name == "" {
		return discoveredCommand{}, false
	}
	return discoveredCommand{
		Name:         name,
		Description:  description,
		Source:       source,
		ArgumentHint: argumentHint,
	}, true
}

func splitFrontmatter(text string) (map[string]string, string) {
	fields := map[string]string{}
	if !strings.HasPrefix(text, "---\n") {
		return fields, text
	}
	rest := text[4:]
	end := strings.Index(rest, "\n---")
	if end < 0 {
		return fields, text
	}
	header := rest[:end]
	body := rest[end+4:]
	for _, line := range strings.Split(header, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(strings.ToLower(key))
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key != "" {
			fields[key] = value
		}
	}
	return fields, body
}

func firstDescriptionLine(body string) string {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "---") {
			continue
		}
		return line
	}
	return ""
}

func sanitizeDiscoveredText(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, value)
	runes := []rune(value)
	if len(runes) > limit {
		runes = runes[:limit]
	}
	return string(runes)
}

func withinDiscoveryRoot(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func countPathSegments(path string) int {
	return len(strings.Split(path, string(filepath.Separator)))
}

// publishDiscoveredCommands sends one durable snapshot before the provider
// starts. The provider's own announcement may follow; clients merge snapshots
// newest-first, so project commands remain available without duplicating the
// provider's built-ins.
func publishDiscoveredCommands(ctx context.Context, cfg wrapConfig, masker *core.EventMasker, write func(context.Context, protocol.Frame) error) error {
	commands := discoverProviderCommands(cfg.Provider, cfg.WorkingDirectory)
	if len(commands) == 0 || write == nil {
		return nil
	}
	payloadCommands := make([]map[string]any, 0, len(commands))
	for _, command := range commands {
		item := map[string]any{
			"name":        command.Name,
			"description": command.Description,
			"source":      command.Source,
		}
		if command.ArgumentHint != "" {
			item["argument_hint"] = command.ArgumentHint
		}
		payloadCommands = append(payloadCommands, item)
	}
	payload, err := json.Marshal(map[string]any{"commands": payloadCommands, "source": "project"})
	if err != nil {
		return err
	}
	event := protocol.Event{
		Type:      "session.commands",
		SessionID: cfg.SessionID,
		Time:      time.Now().UTC().UnixMilli(),
		Payload:   payload,
	}
	if masker != nil {
		masked, err := maskEvent(masker, event)
		if err != nil {
			return err
		}
		event = masked
	}
	return write(ctx, &event)
}
