package protocol

import "testing"

func TestDecodeEncryptedFileRequestsRejectUnsafeComponents(t *testing.T) {
	for _, path := range []string{"name..txt", "dir/file.txt"} {
		request, err := DecodeEncryptedFileReadRequest([]byte(`{"path":"` + path + `"}`))
		if err != nil || request.Path != path {
			t.Fatalf("safe read path %q rejected: %v", path, err)
		}
	}
	if request, err := DecodeEncryptedFileListRequest([]byte(`{"path":"."}`)); err != nil || request.Path != "." {
		t.Fatalf("workspace root rejected: %v", err)
	}
	for _, path := range []string{"", "/absolute", "dir//file", "dir/./file", "dir/../file", "../file", `dir\\file`, "line\nbreak"} {
		raw := []byte(`{"path":`)
		if path == "line\nbreak" {
			raw = append(raw, []byte(`"line\nbreak"}`)...)
		} else {
			raw = append(raw, []byte(`"`+path+`"}`)...)
		}
		if _, err := DecodeEncryptedFileReadRequest(raw); err == nil {
			t.Fatalf("unsafe read path %q accepted", path)
		}
		if _, err := DecodeEncryptedFileListRequest(raw); err == nil {
			t.Fatalf("unsafe list path %q accepted", path)
		}
	}
}
