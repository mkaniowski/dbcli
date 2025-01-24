package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	orientDBBaseURL  = "http://orientdb:2480"
	orientDBUsername = "root"
	orientDBPassword = "rootpwd"
	databaseName     = "dbcli"
)

type CommandBody struct {
	Command    string                 `json:"command"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

type ResultSet struct {
	Result []map[string]interface{} `json:"result"`
}

func ExecuteQuery(command string) (ResultSet, error) {
	var jsonBody ResultSet
	url := fmt.Sprintf("%s/command/%s/sql", orientDBBaseURL, databaseName)
	bodyCmd := CommandBody{
		Command: command,
	}

	body, err := json.Marshal(bodyCmd)
	if err != nil {
		return jsonBody, fmt.Errorf("failed to marshal command body: %w", err)
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		return jsonBody, fmt.Errorf("failed to create command request: %w", err)
	}
	req.SetBasicAuth(orientDBUsername, orientDBPassword)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return jsonBody, fmt.Errorf("failed to execute command: %w", err)
	}
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return jsonBody, fmt.Errorf("failed to read response body: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return jsonBody, fmt.Errorf("command failed with status: %d, body: %s", resp.StatusCode, string(body))
	}

	if err := json.Unmarshal(body, &jsonBody); err != nil {
		return jsonBody, fmt.Errorf("failed to read response body: %w", err)
	}

	return jsonBody, nil
}
