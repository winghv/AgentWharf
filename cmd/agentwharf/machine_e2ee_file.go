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

func emitEncryptedFileResult(ctx context.Context, cfg wrapConfig, decoded *protocol.Command, payload []byte, writeFrame func(protocol.Frame) error) error {
	messageID, err := randomToken()
	if err != nil {
		return err
	}
	sealed, err := cfg.e2eeRuntime.sealEvent(ctx, decoded.SessionID, messageID, "session.file.result", payload)
	if err != nil {
		return err
	}
	return writeFrame(&protocol.Event{Type: "session.file.result", SessionID: decoded.SessionID, Time: time.Now().UTC().UnixMilli(), Payload: sealed})
}

func deliverEncryptedFileRead(ctx context.Context, cfg wrapConfig, command *protocol.Command, writeFrame func(protocol.Frame) error) error {
	if cfg.e2eeRuntime == nil || command == nil || command.Type != protocol.CommandFileRead || writeFrame == nil {
		return errors.New("encrypted file runtime unavailable")
	}
	admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(deliveryCtx context.Context, decoded *protocol.Command) error {
		request, err := protocol.DecodeEncryptedFileReadRequest(decoded.Payload)
		if err != nil {
			return err
		}
		rootPath := cfg.WorkingDirectory
		if rootPath == "" {
			rootPath = "."
		}
		root, err := os.OpenRoot(rootPath)
		if err != nil {
			return errors.New("file workspace unavailable")
		}
		defer root.Close()
		path := filepath.Clean(request.Path)
		linkInfo, err := root.Lstat(path)
		if err != nil || linkInfo.Mode()&os.ModeSymlink != 0 {
			return errors.New("file unavailable")
		}
		file, err := root.Open(path)
		if err != nil {
			return errors.New("file unavailable")
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil || !os.SameFile(linkInfo, info) || !info.Mode().IsRegular() || info.Size() > maxEncryptedFileReadBytes {
			return errors.New("file read rejected")
		}
		raw, err := io.ReadAll(io.LimitReader(file, maxEncryptedFileReadBytes+1))
		if err != nil || len(raw) > maxEncryptedFileReadBytes || !utf8.Valid(raw) {
			return errors.New("file read rejected")
		}
		payload, _ := json.Marshal(map[string]any{"path": request.Path, "content": string(raw), "bytes": len(raw), "truncated": false})
		return emitEncryptedFileResult(deliveryCtx, cfg, decoded, payload, writeFrame)
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

func deliverEncryptedFileList(ctx context.Context, cfg wrapConfig, command *protocol.Command, writeFrame func(protocol.Frame) error) error {
	if cfg.e2eeRuntime == nil || command == nil || command.Type != protocol.CommandFileList || writeFrame == nil {
		return errors.New("encrypted file runtime unavailable")
	}
	admission, err := cfg.e2eeRuntime.deliverCommand(ctx, cfg.SessionID, command, func(deliveryCtx context.Context, decoded *protocol.Command) error {
		request, err := protocol.DecodeEncryptedFileListRequest(decoded.Payload)
		if err != nil {
			return err
		}
		rootPath := cfg.WorkingDirectory
		if rootPath == "" {
			rootPath = "."
		}
		root, err := os.OpenRoot(rootPath)
		if err != nil {
			return errors.New("file workspace unavailable")
		}
		defer root.Close()
		path := filepath.Clean(request.Path)
		linkInfo, err := root.Lstat(path)
		if err != nil || linkInfo.Mode()&os.ModeSymlink != 0 || !linkInfo.IsDir() {
			return errors.New("directory read rejected")
		}
		directory, err := root.Open(path)
		if err != nil {
			return errors.New("directory read rejected")
		}
		defer directory.Close()
		info, err := directory.Stat()
		if err != nil || !os.SameFile(linkInfo, info) || !info.IsDir() {
			return errors.New("directory read rejected")
		}
		entries, err := directory.ReadDir(maxEncryptedFileListEntries + 1)
		if (err != nil && !errors.Is(err, io.EOF)) || len(entries) > maxEncryptedFileListEntries {
			return errors.New("directory read rejected")
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
					return errors.New("directory read rejected")
				}
				node["size_bytes"] = entryInfo.Size()
			}
			nodes = append(nodes, node)
		}
		payload, _ := json.Marshal(map[string]any{"path": request.Path, "nodes": nodes, "truncated": false})
		return emitEncryptedFileResult(deliveryCtx, cfg, decoded, payload, writeFrame)
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
