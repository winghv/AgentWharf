package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/winghv/agentwharf/protocol"
)

const maxEncryptedFileReadBytes = 1024 * 1024
const maxEncryptedFileListEntries = 256

var (
	errInvalidEncryptedFileRequest = errors.New("invalid encrypted file request")
	errEncryptedFileUnavailable    = errors.New("encrypted file unavailable")
)

func emitEncryptedFileResult(decoded *protocol.Command, payload []byte, writeFrame func(protocol.Frame) error) error {
	// The hubConnection writer owns encryption for every event. Passing a sealed
	// carrier here would encrypt it a second time and expose only the inner
	// carrier after one client decrypt.
	return writeFrame(&protocol.Event{Type: "session.file.result", SessionID: decoded.SessionID, Time: time.Now().UTC().UnixMilli(), Payload: payload})
}

func rejectUnavailableEncryptedFileCommand(ctx context.Context, cfg wrapConfig, command *protocol.Command, inspect func(json.RawMessage) ([]byte, error), writeFrame func(protocol.Frame) error) (bool, error) {
	err := cfg.e2eeRuntime.executor.InspectWire(ctx, cfg.SessionID, command.CommandID, string(command.Type), command.Payload, func(payload json.RawMessage) error {
		result, inspectErr := inspect(payload)
		clear(result)
		return inspectErr
	})
	if err == nil {
		return false, nil
	}
	reason := ""
	if errors.Is(err, errInvalidEncryptedFileRequest) {
		reason = "invalid_file_request"
	} else if errors.Is(err, errEncryptedFileUnavailable) {
		reason = "file_unavailable"
	}
	if reason == "" {
		return false, err
	}
	return true, writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: protocol.AckRejected, Reason: reason})
}

func encryptedFileReadResult(cfg wrapConfig, payload json.RawMessage) ([]byte, error) {
	request, err := protocol.DecodeEncryptedFileReadRequest(payload)
	if err != nil {
		return nil, errInvalidEncryptedFileRequest
	}
	rootPath := cfg.WorkingDirectory
	if rootPath == "" {
		rootPath = "."
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, errEncryptedFileUnavailable
	}
	defer root.Close()
	path := filepath.Clean(request.Path)
	linkInfo, err := root.Lstat(path)
	if err != nil || linkInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errEncryptedFileUnavailable
	}
	file, err := root.Open(path)
	if err != nil {
		return nil, errEncryptedFileUnavailable
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !os.SameFile(linkInfo, info) || !info.Mode().IsRegular() || info.Size() > maxEncryptedFileReadBytes {
		return nil, errEncryptedFileUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxEncryptedFileReadBytes+1))
	if err != nil || len(raw) > maxEncryptedFileReadBytes || !utf8.Valid(raw) {
		clear(raw)
		return nil, errEncryptedFileUnavailable
	}
	defer clear(raw)
	result, err := json.Marshal(map[string]any{"path": request.Path, "content": string(raw), "bytes": len(raw), "truncated": false})
	if err != nil {
		return nil, errEncryptedFileUnavailable
	}
	return result, nil
}

func deliverEncryptedFileRead(ctx context.Context, cfg wrapConfig, command *protocol.Command, writeFrame func(protocol.Frame) error) error {
	if cfg.e2eeRuntime == nil || command == nil || command.Type != protocol.CommandFileRead || writeFrame == nil {
		return errors.New("encrypted file runtime unavailable")
	}
	if rejected, err := rejectUnavailableEncryptedFileCommand(ctx, cfg, command, func(payload json.RawMessage) ([]byte, error) {
		return encryptedFileReadResult(cfg, payload)
	}, writeFrame); rejected || err != nil {
		return err
	}
	admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(_ context.Context, decoded *protocol.Command) error {
		payload, err := encryptedFileReadResult(cfg, decoded.Payload)
		if err != nil {
			return err
		}
		defer clear(payload)
		return emitEncryptedFileResult(decoded, payload, writeFrame)
	})
	if err != nil {
		return errors.New("encrypted file read failed")
	}
	if admission.State != "completed" {
		return errors.New("encrypted file read outcome unknown")
	}
	status := protocol.AckAccepted
	if !admission.Execute {
		status = protocol.AckDuplicate
	}
	return writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: status})
}

func encryptedFileListResult(cfg wrapConfig, payload json.RawMessage) ([]byte, error) {
	request, err := protocol.DecodeEncryptedFileListRequest(payload)
	if err != nil {
		return nil, errInvalidEncryptedFileRequest
	}
	rootPath := cfg.WorkingDirectory
	if rootPath == "" {
		rootPath = "."
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, errEncryptedFileUnavailable
	}
	defer root.Close()
	path := filepath.Clean(request.Path)
	linkInfo, err := root.Lstat(path)
	if err != nil || linkInfo.Mode()&os.ModeSymlink != 0 || !linkInfo.IsDir() {
		return nil, errEncryptedFileUnavailable
	}
	directory, err := root.Open(path)
	if err != nil {
		return nil, errEncryptedFileUnavailable
	}
	defer directory.Close()
	info, err := directory.Stat()
	if err != nil || !os.SameFile(linkInfo, info) || !info.IsDir() {
		return nil, errEncryptedFileUnavailable
	}
	entries, err := directory.ReadDir(maxEncryptedFileListEntries + 1)
	if (err != nil && !errors.Is(err, io.EOF)) || len(entries) > maxEncryptedFileListEntries {
		return nil, errEncryptedFileUnavailable
	}
	nodes := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		nodeType := "file"
		if entry.Type()&os.ModeSymlink != 0 {
			nodeType = "symlink"
		} else if entry.IsDir() {
			nodeType = "directory"
		}
		node := map[string]any{"name": entry.Name(), "type": nodeType}
		if nodeType == "file" {
			entryInfo, infoErr := entry.Info()
			if infoErr != nil || !entryInfo.Mode().IsRegular() {
				return nil, errEncryptedFileUnavailable
			}
			node["size_bytes"] = entryInfo.Size()
		}
		nodes = append(nodes, node)
	}
	result, err := json.Marshal(map[string]any{"path": request.Path, "nodes": nodes, "truncated": false})
	if err != nil {
		return nil, errEncryptedFileUnavailable
	}
	return result, nil
}

func deliverEncryptedFileList(ctx context.Context, cfg wrapConfig, command *protocol.Command, writeFrame func(protocol.Frame) error) error {
	if cfg.e2eeRuntime == nil || command == nil || command.Type != protocol.CommandFileList || writeFrame == nil {
		return errors.New("encrypted file runtime unavailable")
	}
	if rejected, err := rejectUnavailableEncryptedFileCommand(ctx, cfg, command, func(payload json.RawMessage) ([]byte, error) {
		return encryptedFileListResult(cfg, payload)
	}, writeFrame); rejected || err != nil {
		return err
	}
	admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(_ context.Context, decoded *protocol.Command) error {
		payload, err := encryptedFileListResult(cfg, decoded.Payload)
		if err != nil {
			return err
		}
		defer clear(payload)
		return emitEncryptedFileResult(decoded, payload, writeFrame)
	})
	if err != nil {
		return errors.New("encrypted file list failed")
	}
	if admission.State != "completed" {
		return errors.New("encrypted file list outcome unknown")
	}
	status := protocol.AckAccepted
	if !admission.Execute {
		status = protocol.AckDuplicate
	}
	return writeFrame(&protocol.CommandAck{CommandID: command.CommandID, Status: status})
}
