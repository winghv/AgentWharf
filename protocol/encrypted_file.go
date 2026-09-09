package protocol

import (
	"encoding/json"
	"errors"
	"strings"
)

type EncryptedFileReadRequest struct {
	Path string `json:"path"`
}
type EncryptedFileListRequest struct {
	Path string `json:"path"`
}

func safeEncryptedPath(path string, allowDot bool) bool {
	if path == "." {
		return allowDot
	}
	if path == "" || len(path) > 4096 || strings.HasPrefix(path, "/") || strings.Contains(path, "\\") || strings.ContainsAny(path, "\x00\r\n") {
		return false
	}
	for _, component := range strings.Split(path, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return true
}
func DecodeEncryptedFileReadRequest(data json.RawMessage) (EncryptedFileReadRequest, error) {
	fields, err := strictObject(data)
	if err != nil || len(fields) != 1 || fields["path"] == nil {
		return EncryptedFileReadRequest{}, errors.New("invalid encrypted file request")
	}
	var path string
	if json.Unmarshal(fields["path"], &path) != nil || !safeEncryptedPath(path, false) {
		return EncryptedFileReadRequest{}, errors.New("unsafe encrypted file path")
	}
	return EncryptedFileReadRequest{Path: path}, nil
}
func DecodeEncryptedFileListRequest(data json.RawMessage) (EncryptedFileListRequest, error) {
	fields, err := strictObject(data)
	if err != nil || len(fields) != 1 || fields["path"] == nil {
		return EncryptedFileListRequest{}, errors.New("invalid encrypted directory request")
	}
	var path string
	if json.Unmarshal(fields["path"], &path) != nil || !safeEncryptedPath(path, true) {
		return EncryptedFileListRequest{}, errors.New("unsafe encrypted directory path")
	}
	return EncryptedFileListRequest{Path: path}, nil
}
