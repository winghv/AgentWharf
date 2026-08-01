//go:build !linux

package core

import (
	"context"
	"errors"
)

var (
	ErrChildConfidentialityUnavailable = errors.New("child confidentiality boundary is unavailable")
	ErrChildConfidentialityUnproven    = errors.New("child confidentiality boundary is unproven")
)

type ChildConfidentialityReport struct{}

// Non-Linux platforms have no approved child-confidentiality proof in this
// task. Callers must keep multi-Worker capability disabled.
func ProbeChildConfidentiality(context.Context) (ChildConfidentialityReport, error) {
	return ChildConfidentialityReport{}, ErrChildConfidentialityUnavailable
}
