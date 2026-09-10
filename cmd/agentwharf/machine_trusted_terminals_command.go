package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const trustedTerminalsUsage = "usage: wharf trusted-terminals [status]"

// runTrustedTerminalsCommand reports the effective account-terminal trust for
// this machine. The flag is owner-controlled on the platform, so the command is
// read-only: a machine-local write would silently diverge from the value the
// daemon polls. Enabling or disabling it belongs to the Web Console.
func runTrustedTerminalsCommand(ctx context.Context, args []string, stdout io.Writer) error {
	if len(args) == 1 && (args[0] == "on" || args[0] == "off") {
		return errors.New("wharf trusted-terminals: this machine setting is owner-controlled; change it for this machine from the Web Console")
	}
	if len(args) > 1 || (len(args) == 1 && args[0] != "status") {
		return errors.New(trustedTerminalsUsage)
	}
	credential, err := loadMachineCredential()
	if err != nil {
		if errors.Is(err, errMachineCredentialNotFound) {
			return errors.New("wharf trusted-terminals: no local machine pairing found; run wharf to pair first")
		}
		return err
	}
	enabled, err := fetchTrustAccountTerminals(ctx, &http.Client{Timeout: 20 * time.Second}, credential)
	if err != nil {
		return fmt.Errorf("wharf trusted-terminals: %w", err)
	}
	override := strings.TrimSpace(os.Getenv("AGENTWHARF_TRUST_ACCOUNT_TERMINALS")) == "1"
	_, _ = fmt.Fprintf(stdout, "trusted_account_terminals: %t\n", enabled || override)
	_, _ = fmt.Fprintf(stdout, "platform_setting: %t\n", enabled)
	if override {
		_, _ = fmt.Fprintln(stdout, "local_override: AGENTWHARF_TRUST_ACCOUNT_TERMINALS=1")
	}
	if !enabled {
		_, _ = fmt.Fprintln(stdout, "enable it for this machine from the Web Console (owner account), then restart wharf serve")
	}
	return nil
}
