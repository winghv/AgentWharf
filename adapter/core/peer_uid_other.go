//go:build !linux && !darwin

package core

import "errors"

func peerUID(int) (uint32, error) {
	return 0, errors.New("unix peer credentials are unsupported on this platform")
}
