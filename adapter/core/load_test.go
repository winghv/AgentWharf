package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAdapterMetricsAreBoundedAndSecretFree(t *testing.T) {
	metrics := NewAdapterMetrics()
	metrics.SetWorkerCounts(-1, 2, -3)
	metrics.IncReceiptFailure()
	metrics.IncMaskedEvent()
	snapshot := metrics.Snapshot()
	if snapshot.Workers != 0 || snapshot.ActiveWorkers != 2 || snapshot.QueuedWorkers != 0 || snapshot.ReceiptFailures != 1 || snapshot.MaskedEvents != 1 {
		t.Fatalf("metric snapshot = %+v", snapshot)
	}
	output := snapshot.Prometheus()
	if !strings.Contains(output, "agentwharf_adapter_active_workers 2") || strings.Contains(output, "session") || strings.Contains(output, "secret") {
		t.Fatalf("unsafe metric output = %q", output)
	}
	var nilMetrics *AdapterMetrics
	if got := nilMetrics.Snapshot(); got != (AdapterMetricSnapshot{}) {
		t.Fatalf("nil metric snapshot = %+v", got)
	}
}

func TestAdapterObservabilityHandlerRestrictsDiagnostics(t *testing.T) {
	metrics := NewAdapterMetrics()
	metrics.SetWorkerCounts(2, 1, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := NewAdapterObservabilityHandler("adapter-diagnostic-token", metrics, next)

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/metrics", nil)
	request.RemoteAddr = "192.0.2.10:1000"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("remote metrics status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1/metrics", nil)
	request.RemoteAddr = "127.0.0.1:1000"
	request.Header.Set("Authorization", "Bearer adapter-diagnostic-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "agentwharf_adapter_workers 2") || strings.Contains(response.Body.String(), "adapter-diagnostic-token") {
		t.Fatalf("authorized metrics response = %d %q", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1/debug/pprof/", nil)
	request.RemoteAddr = "[::1]:1000"
	request.Header.Set("Authorization", "Bearer adapter-diagnostic-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "profile") {
		t.Fatalf("authorized pprof response = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "http://127.0.0.1/metrics", nil)
	request.RemoteAddr = "127.0.0.1:1000"
	request.Header.Set("Authorization", "Bearer adapter-diagnostic-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST metrics status = %d", response.Code)
	}

	request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1/ws", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("fallback status = %d", response.Code)
	}
}

func TestAdapterObservabilityPprofRoutesAndNilMetrics(t *testing.T) {
	handler := NewAdapterObservabilityHandler("token", nil, nil)
	for _, path := range []string{"/debug/pprof/cmdline", "/debug/pprof/symbol", "/debug/pprof/unknown", "/debug/pprof/a/b"} {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+path, nil)
		request.RemoteAddr = "127.0.0.1:1000"
		request.Header.Set("Authorization", "Bearer token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK && response.Code != http.StatusNotFound {
			t.Fatalf("pprof %s status = %d", path, response.Code)
		}
	}
}

func TestAdapterCredentialAndReceiptBoundaries(t *testing.T) {
	credential, err := NewSessionCredential("adapter-secret", SessionCredentialMetadata{
		SessionID: "ses_load", Lineage: SessionCredentialLineage{Kind: "target", AttachID: "att"},
		Generation: 1, ExpiresAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("credential = %v", err)
	}
	if credential.String() != "[session credential redacted]" || credential.GoString() != credential.String() {
		t.Fatalf("credential formatting leaked value: %q %q", credential.String(), credential.GoString())
	}
	if _, err := json.Marshal(credential); !errors.Is(err, ErrSessionCredentialNotSerializable) {
		t.Fatalf("credential JSON error = %v", err)
	}
	if _, err := credential.MarshalText(); !errors.Is(err, ErrSessionCredentialNotSerializable) {
		t.Fatalf("credential text error = %v", err)
	}
	if err := credential.validate("other", time.Now()); !errors.Is(err, ErrSessionCredentialMismatch) {
		t.Fatalf("credential mismatch = %v", err)
	}
}

func TestAdapterRotationAndWorkerWrappersFailClosed(t *testing.T) {
	if _, err := NewSessionWorker(SessionWorkerConfig{}); !errors.Is(err, ErrInvalidSessionWorkerConfig) {
		t.Fatalf("invalid worker = %v", err)
	}
	if err := (*SessionWorker)(nil).ActivateCredentialRotation(CredentialRotationReceipt{}); !errors.Is(err, ErrCredentialRotationUnavailable) {
		t.Fatalf("nil activation = %v", err)
	}
	if _, err := (*SessionWorker)(nil).RetryCredentialActivation("rotation"); !errors.Is(err, ErrCredentialRotationUnavailable) {
		t.Fatalf("nil retry = %v", err)
	}
	if err := (*SessionWorker)(nil).ReconnectCredential(2, 1); !errors.Is(err, ErrCredentialRotationUnavailable) {
		t.Fatalf("nil reconnect = %v", err)
	}
	if _, err := (*SessionWorker)(nil).CredentialRecoveryPermit(); !errors.Is(err, ErrCredentialRotationUnavailable) {
		t.Fatalf("nil recovery permit = %v", err)
	}
	if err := (*SessionWorker)(nil).Run(context.Background()); !errors.Is(err, ErrInvalidSessionWorkerConfig) {
		t.Fatalf("nil run = %v", err)
	}
}

func TestAdapterRotationAndWorkerWrappersHappyPath(t *testing.T) {
	worker := rotationWorker(t, "ses_load_rotation", &testDurableReceiptGate{})
	pending := rotationCredential(t, worker.SessionID(), 2)
	if err := worker.PrepareCredentialRotation("rot_load", pending); err != nil {
		t.Fatalf("PrepareCredentialRotation() error = %v", err)
	}
	if err := worker.PrepareCredentialRotation("rot_load", pending); err != nil {
		t.Fatalf("idempotent PrepareCredentialRotation() error = %v", err)
	}
	receipt, err := worker.AcknowledgeCredentialPossession("rot_load", 1)
	if err != nil {
		t.Fatalf("AcknowledgeCredentialPossession() error = %v", err)
	}
	if err := worker.ActivateCredentialRotation(receipt); err != nil {
		t.Fatalf("ActivateCredentialRotation() error = %v", err)
	}
	active, err := worker.RetryCredentialActivation("rot_load")
	if err != nil || active.Status != CredentialRotationActive {
		t.Fatalf("RetryCredentialActivation() = %+v, %v", active, err)
	}
	if err := worker.ReconnectCredential(2, active.Generation); err != nil {
		t.Fatalf("ReconnectCredential() error = %v", err)
	}
	permit, err := worker.CredentialRecoveryPermit()
	if err != nil || permit.SessionID != worker.SessionID() || permit.Epoch != 2 || permit.Generation != 1 {
		t.Fatalf("CredentialRecoveryPermit() = %+v, %v", permit, err)
	}
	worker.MarkCredentialAuthorityLost()
	if _, err := worker.CredentialRecoveryPermit(); !errors.Is(err, ErrCredentialAuthorityLost) {
		t.Fatalf("authority-lost recovery permit = %v", err)
	}
}

func TestAdapterGroupConfigurationAndAuthorityReplacement(t *testing.T) {
	if _, err := NewGroupSupervisor(GroupSupervisorConfig{}); !errors.Is(err, ErrInvalidGroupSupervisorConfig) {
		t.Fatalf("empty group config = %v", err)
	}
	if _, err := NewGroupSupervisor(GroupSupervisorConfig{MaxWorkers: 1}); !errors.Is(err, ErrInvalidGroupSupervisorConfig) {
		t.Fatalf("missing lease group config = %v", err)
	}
	if (*GroupSupervisor)(nil).WorkerCount() != 0 || (*GroupSupervisor)(nil).Run(context.Background(), "x") == nil {
		t.Fatal("nil group supervisor did not fail closed")
	}
	recovery, store := validGroupWorkerRecoveryWithStore("worker_replace", "ses_replace", 7)
	replacement := recovery.Authority.receipt
	replacement.AcceptedFence = 2
	store.connection.AcceptedFence = 2
	if err := recovery.Authority.lifecycle.Replace(replacement); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if err := recovery.Authority.lifecycle.VerifyConnectionAuthority(context.Background(), replacement); err != nil {
		t.Fatalf("VerifyConnectionAuthority(replacement) error = %v", err)
	}
	recovery.Authority.lifecycle.Revoke()
	if err := recovery.Authority.lifecycle.Replace(replacement); !errors.Is(err, ErrRecoveryAuthorityLost) {
		t.Fatalf("Replace(revoked) error = %v", err)
	}
}

func TestAdapterProviderEnvironmentRejectsCredentialNames(t *testing.T) {
	if safeProviderEnvName("HUB_TOKEN") || safeProviderEnvName("SESSION_SECRET") || safeProviderEnvName("AGENTWHARF_WRAP_HELPER") != true {
		t.Fatal("credential environment allowlist is not fail-closed")
	}
	values := providerEnvironment("/bin/echo", []string{"HUB_TOKEN=secret", "PATH=/bin", "AGENTWHARF_WRAP_HELPER=ok"})
	joined := strings.Join(values, "\n")
	if strings.Contains(joined, "HUB_TOKEN") || !strings.Contains(joined, "PATH=") || !strings.Contains(joined, "AGENTWHARF_WRAP_HELPER=ok") {
		t.Fatalf("provider environment = %q", joined)
	}
}
