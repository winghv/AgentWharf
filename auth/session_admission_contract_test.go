package auth

import (
	"testing"
	"time"

	"github.com/winghv/agentwharf/store"
)

func TestSessionAdmissionContract(t *testing.T) {
	claim := SessionAdmissionClaim{SessionID: "ses_target", Provider: "claude-code", ExpiresAt: time.Now().Add(time.Minute)}
	exact := Principal{Subject: "user", Scopes: []Scope{SessionControl(claim.SessionID)}}
	decision, err := EvaluateSessionAdmission(SessionAdmissionRequest{Principal: exact, Claim: claim, Truth: store.SessionAdmissionTruth{SessionID: claim.SessionID}})
	if err != nil || decision.Mode != SessionAdmissionAttachOnly || decision.MayMutate {
		t.Fatalf("fresh admission = %+v, %v", decision, err)
	}
	if !decision.Allows(SessionAdmissionAttach) || !decision.Allows(SessionAdmissionStatus) {
		t.Fatal("attach_only omitted attach or status")
	}
	for _, action := range []SessionAdmissionAction{SessionAdmissionHistory, SessionAdmissionSend, SessionAdmissionSettings, SessionAdmissionRunControl, SessionAdmissionPermission, SessionAdmissionRotation} {
		if decision.Allows(action) {
			t.Fatalf("attach_only unexpectedly allowed %s", action)
		}
	}
	decision, err = EvaluateSessionAdmission(SessionAdmissionRequest{Principal: exact, Claim: claim, Truth: store.SessionAdmissionTruth{SessionID: claim.SessionID, Exists: true, Complete: true, Live: true}})
	if err != nil || decision.Mode != SessionAdmissionCurrent || !decision.MayMutate {
		t.Fatalf("existing admission = %+v, %v", decision, err)
	}
	for _, action := range []SessionAdmissionAction{
		SessionAdmissionAttach,
		SessionAdmissionStatus,
		SessionAdmissionHistory,
		SessionAdmissionSend,
		SessionAdmissionSettings,
		SessionAdmissionRunControl,
		SessionAdmissionPermission,
		SessionAdmissionRotation,
	} {
		if !decision.Allows(action) {
			t.Fatalf("current admission unexpectedly rejected %s", action)
		}
	}
	if decision.Allows(SessionAdmissionAction("future_sensitive_action")) {
		t.Fatal("current admission unexpectedly allowed an unknown action")
	}
	for _, truth := range []store.SessionAdmissionTruth{
		{SessionID: claim.SessionID, Exists: true},
		{SessionID: claim.SessionID, Exists: true, Complete: true, Terminal: true},
		{SessionID: claim.SessionID, Exists: true, Complete: true, Conflicting: true},
		{SessionID: claim.SessionID, Exists: true, Complete: true},
		{SessionID: claim.SessionID, Complete: true},
	} {
		if _, err := EvaluateSessionAdmission(SessionAdmissionRequest{Principal: exact, Claim: claim, Truth: truth}); err == nil {
			t.Fatal("invalid Store truth unexpectedly admitted")
		}
	}
	for _, principal := range []Principal{
		{Subject: "api", Scopes: []Scope{API()}},
		{Subject: "group", Scopes: []Scope{GroupControl("group")}},
		{Subject: "other", Scopes: []Scope{SessionControl("ses_other")}},
	} {
		if _, err := EvaluateSessionAdmission(SessionAdmissionRequest{Principal: principal, Claim: claim, Truth: store.SessionAdmissionTruth{SessionID: claim.SessionID}}); err == nil {
			t.Fatal("non-exact principal unexpectedly admitted")
		}
	}
}
