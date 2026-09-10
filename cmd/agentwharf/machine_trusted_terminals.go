package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"
)

// fetchTrustAccountTerminals reads the owner-controlled per-machine flag. On any
// error the caller must treat trust as disabled (default off).
func fetchTrustAccountTerminals(ctx context.Context, client *http.Client, credential machineCredential) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	endpoint, err := cloudAPIEndpoint(credential.CloudAPIURL, "/machines/"+url.PathEscape(credential.MachineID)+"/trusted-terminals/endpoint")
	if err != nil {
		return false, err
	}
	status, body, err := getCloudAPIJSON(ctx, client, endpoint, credential.MachineToken)
	if err != nil || status != http.StatusOK {
		return false, errors.New("trusted terminal setting unavailable")
	}
	var response struct {
		Data struct {
			Enabled bool `json:"enabled"`
		} `json:"data"`
	}
	if decodeCloudAPIJSON(body, &response) != nil {
		return false, errors.New("invalid trusted terminal setting")
	}
	return response.Data.Enabled, nil
}
