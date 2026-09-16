package main

import "strings"

// A missing secret directory alone is not evidence of an Own Machine endpoint.
func ownMachineProviderEnvironment(cfg wrapConfig) bool {
	return cfg.e2eeRuntime != nil && cfg.SecretDir == "" && cfg.ProviderCredential == nil
}

func internalProviderEnvironmentName(name string) bool {
	name = strings.ToUpper(name)
	for _, prefix := range []string{"AGENTWHARF_", "WHARF_", "SUPERWHV_"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// Remove internal values even if the operator has aliased them under a different
// variable name. This does not export local environment through the platform.
func localProviderSecrets(cfg wrapConfig, parent []string) []string {
	secrets := []string{}
	if cfg.AdapterToken != "" {
		secrets = append(secrets, cfg.AdapterToken)
	}
	for _, entry := range parent {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || value == "" {
			continue
		}
		upper := strings.ToUpper(name)
		for _, marker := range []string{"TOKEN", "KEY", "SECRET", "PASSWORD", "CREDENTIAL", "DSN", "PROXY", "BASE_URL"} {
			if strings.Contains(upper, marker) {
				secrets = append(secrets, value)
				break
			}
		}
	}
	return secrets
}

func localProviderEnvironment(cfg wrapConfig, parent []string) []string {
	internalValues := []string{}
	if cfg.AdapterToken != "" {
		internalValues = append(internalValues, cfg.AdapterToken)
	}
	for _, entry := range parent {
		name, value, ok := strings.Cut(entry, "=")
		if ok && value != "" && internalProviderEnvironmentName(name) {
			upper := strings.ToUpper(name)
			for _, marker := range []string{"TOKEN", "KEY", "SECRET", "PASSWORD", "CREDENTIAL", "DSN"} {
				if strings.Contains(upper, marker) {
					internalValues = append(internalValues, value)
					break
				}
			}
		}
	}
	result := make([]string, 0, len(parent))
	for _, entry := range parent {
		name, value, ok := strings.Cut(entry, "=")
		if !ok || name == "" || internalProviderEnvironmentName(name) {
			continue
		}
		blocked := false
		for _, secret := range internalValues {
			if strings.Contains(value, secret) {
				blocked = true
				break
			}
		}
		if !blocked {
			result = append(result, entry)
		}
	}
	return result
}
