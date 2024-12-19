package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

const (
	orientDBBaseURL  = "http://localhost:2480" // Replace with your OrientDB REST endpoint
	orientDBUsername = "root"
	orientDBPassword = "rootpwd"
	databaseName     = "dbcli"
	batchSize        = 7400 // Number of records per batch
)

// BatchOperation represents an operation in the batch request
type BatchOperation struct {
	Type     string                 `json:"type"`
	Language string                 `json:"language,omitempty"`
	Command  string                 `json:"command,omitempty"`
	Record   map[string]interface{} `json:"record,omitempty"`
}

// BatchRequest represents a batch request
type BatchRequest struct {
	Transaction bool             `json:"transaction"`
	Operations  []BatchOperation `json:"operations"`
}

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import [data directory]",
	Short: "Import data from popularity and taxonomy files into OrientDB",
	Long: `Import vertices and edges from the given data directory into OrientDB.
The directory should contain:
- popularity_iw.csv
- taxonomy_iw.csv
`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dataDir := args[0]

		// Ensure the database exists
		if err := ensureDatabaseExists(); err != nil {
			log.Fatalf("Failed to ensure database existence: %v", err)
		}

		// Create schema outside of a transaction
		if err := createSchema(); err != nil {
			log.Fatalf("Failed to create schema: %v", err)
		}

		// Load popularity data
		popularityMap, popularityVertices := loadPopularity(filepath.Join(dataDir, "popularity_iw.csv"))

		// Load taxonomy edges and gather vertices
		taxonomyVertices, edgePairs := loadEdges(filepath.Join(dataDir, "taxonomy_iw.csv"))

		// Merge all vertices: from popularity and taxonomy
		allVertices := make(map[string]struct{})
		for v := range popularityVertices {
			allVertices[v] = struct{}{}
		}
		for v := range taxonomyVertices {
			allVertices[v] = struct{}{}
		}

		// Insert all vertices in batches
		if err := insertAllVertices(allVertices, popularityMap); err != nil {
			log.Fatalf("Failed to insert vertices: %v", err)
		}

		// Create a name->rid map after all vertices are inserted
		vertexRIDMap, err := fetchAllVertexRIDs()
		if err != nil {
			log.Fatalf("Failed to fetch vertex RIDs: %v", err)
		}

		// Insert edges in batches using known RIDs
		if err := insertAllEdges(edgePairs, vertexRIDMap); err != nil {
			log.Fatalf("Failed to insert edges: %v", err)
		}

		fmt.Println("Data import completed successfully!")
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
}

// ensureDatabaseExists checks if the database exists and creates it if not
func ensureDatabaseExists() error {
	dbCheckURL := fmt.Sprintf("%s/database/%s", orientDBBaseURL, databaseName)

	req, err := http.NewRequest("GET", dbCheckURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(orientDBUsername, orientDBPassword)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to check database: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("Database already exists.")
		return nil
	}

	// Create the database if it doesn't exist
	dbCreateURL := fmt.Sprintf("%s/database/%s/plocal", orientDBBaseURL, databaseName)
	req, err = http.NewRequest("POST", dbCreateURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(orientDBUsername, orientDBPassword)

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create database, status: %d, body: %s", resp.StatusCode, string(body))
	}

	fmt.Println("Database created successfully.")
	return nil
}

func createSchema() error {
	// The class V and E are system classes and already exist.
	// Just ensure the property and index are created.
	if err := runSQLCommand("CREATE PROPERTY V.name STRING"); err != nil {
		// Ignore if property already exists
		log.Printf("Warning: could not create property V.name: %v", err)
	}

	if err := runSQLCommand("CREATE INDEX V.name UNIQUE"); err != nil {
		// Ignore if index already exists
		log.Printf("Warning: could not create index on V.name: %v", err)
	}

	return nil
}

func runSQLCommand(command string) error {
	url := fmt.Sprintf("%s/command/%s/sql", orientDBBaseURL, databaseName)
	data := "command=" + urlEncode(command)

	req, err := http.NewRequest("POST", url, strings.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create command request: %w", err)
	}

	req.SetBasicAuth(orientDBUsername, orientDBPassword)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute command: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("command failed with status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

func urlEncode(s string) string {
	return strings.ReplaceAll(s, " ", "%20")
}

// loadPopularity loads popularity data and vertex names
func loadPopularity(filePath string) (map[string]int, map[string]struct{}) {
	popularityMap := make(map[string]int)
	allVertices := make(map[string]struct{})

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open popularity file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			log.Printf("Skipping invalid line in popularity file: %s", line)
			continue
		}

		name := strings.Trim(parts[0], `"`)
		popularity, err := strconv.Atoi(parts[1])
		if err != nil {
			log.Printf("Skipping line due to invalid popularity value: %s", line)
			continue
		}
		popularityMap[name] = popularity
		allVertices[name] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading popularity file: %v", err)
	}

	return popularityMap, allVertices
}

// loadEdges loads edges and returns vertex names and edge pairs
func loadEdges(filePath string) (map[string]struct{}, [][2]string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open edges file: %v", err)
	}
	defer file.Close()

	allVertices := make(map[string]struct{})
	edgePairs := make([][2]string, 0, 10000)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) != 2 {
			log.Printf("Skipping invalid line in edges file: %s", line)
			continue
		}

		from := strings.Trim(parts[0], `"`)
		to := strings.Trim(parts[1], `"`)

		allVertices[from] = struct{}{}
		allVertices[to] = struct{}{}
		edgePairs = append(edgePairs, [2]string{from, to})
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading edges file: %v", err)
	}

	return allVertices, edgePairs
}

// insertAllVertices inserts all vertices using batch operations
func insertAllVertices(allVertices map[string]struct{}, popularityMap map[string]int) error {
	operations := make([]BatchOperation, 0, batchSize)

	for name := range allVertices {
		pop := 0
		if p, ok := popularityMap[name]; ok {
			pop = p
		}
		op := BatchOperation{
			Type: "c",
			Record: map[string]interface{}{
				"@class":     "V",
				"name":       name,
				"popularity": pop,
			},
		}
		operations = append(operations, op)
		if len(operations) == batchSize {
			if err := sendBatchRequest(operations, true); err != nil {
				return err
			}
			operations = operations[:0]
		}
	}

	if len(operations) > 0 {
		return sendBatchRequest(operations, true)
	}

	return nil
}

func insertAllEdges(edgePairs [][2]string, vertexRIDMap map[string]string) error {
	operations := make([]BatchOperation, 0, batchSize)

	for _, pair := range edgePairs {
		fromName := pair[0]
		toName := pair[1]

		fromRID, okFrom := vertexRIDMap[fromName]
		if !okFrom {
			// This should not happen if we created all vertices first
			log.Printf("Warning: from vertex not found: %s", fromName)
			continue
		}

		toRID, okTo := vertexRIDMap[toName]
		if !okTo {
			// This should not happen if we created all vertices first
			log.Printf("Warning: to vertex not found: %s", toName)
			continue
		}

		// Create the edge
		op := BatchOperation{
			Type: "c",
			Record: map[string]interface{}{
				"@class": "E",
				"out":    fromRID,
				"in":     toRID,
			},
		}
		operations = append(operations, op)

		if len(operations) >= batchSize {
			if err := sendBatchRequest(operations, true); err != nil {
				return err
			}
			operations = operations[:0]
		}
	}

	if len(operations) > 0 {
		return sendBatchRequest(operations, true)
	}

	return nil
}

// fetchAllVertexRIDs fetches all vertices with their RIDs
func fetchAllVertexRIDs() (map[string]string, error) {
	url := fmt.Sprintf("%s/query/%s/sql/SELECT name,@rid FROM V LIMIT -1", orientDBBaseURL, databaseName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(orientDBUsername, orientDBPassword)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to query vertices: status %d, body: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Result []struct {
			Name string `json:"name"`
			Rid  string `json:"@rid"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	m := make(map[string]string, len(result.Result))
	for _, r := range result.Result {
		m[r.Name] = r.Rid
	}

	return m, nil
}

// sendBatchRequest sends a batch of operations to the OrientDB REST API
func sendBatchRequest(operations []BatchOperation, transaction bool) error {
	request := BatchRequest{
		Transaction: transaction,
		Operations:  operations,
	}
	jsonData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal batch request: %w", err)
	}

	url := fmt.Sprintf("%s/batch/%s", orientDBBaseURL, databaseName)
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create batch request: %w", err)
	}
	req.SetBasicAuth(orientDBUsername, orientDBPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("batch insert failed with status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}
