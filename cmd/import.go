package cmd

import (
	"bytes"
	"dbcli/importer"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

const (
	orientDBBaseURL  = "http://localhost:2480" // Replace with your OrientDB REST endpoint
	orientDBUsername = "root"
	orientDBPassword = "rootpwd"
	databaseName     = "dbcli"
	batchSize        = 20000 // Number of records per batch
	workers          = 6     // Number of workers for parallel processing
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
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dataDir := args[0]

		// Ensure the database exists
		if err := ensureDatabaseExists(); err != nil {
			log.Fatalf("Failed to ensure database existence: %v", err)
		}

		// Create schema
		if err := createSchema(); err != nil {
			log.Fatalf("Failed to create schema: %v", err)
		}

		startImport := time.Now()
		startLoadPopularity := time.Now()

		// Load popularity data
		popularityMap, popularityVertices := importer.LoadPopularity(filepath.Join(dataDir, "popularity_iw.csv"))

		elapsedLoadPopularity := time.Since(startLoadPopularity)
		startLoadTaxonomy := time.Now()

		// Load taxonomy edges and gather vertices
		taxonomyVertices, edgePairs := importer.LoadEdges(filepath.Join(dataDir, "taxonomy_iw.csv"))

		elapsedLoadTaxonomy := time.Since(startLoadTaxonomy)

		startMerge := time.Now()

		// Merge vertices
		allVertices := mergeVertices(popularityVertices, taxonomyVertices)

		elapsedMerge := time.Since(startMerge)

		startInsertVertecies := time.Now()

		// Insert all vertices in batches
		if err := insertAllVertices(allVertices, popularityMap); err != nil {
			log.Fatalf("Failed to insert vertices: %v", err)
		}

		elapsedInsertVertices := time.Since(startInsertVertecies)

		startFetchVertexRIDs := time.Now()

		// Create a name->rid map after all vertices are inserted
		vertexRIDMap, err := fetchAllVertexRIDs()
		if err != nil {
			log.Fatalf("Failed to fetch vertex RIDs: %v", err)
		}

		elapsedFetchVertexRIDs := time.Since(startFetchVertexRIDs)

		startInsertEdges := time.Now()

		// Insert edges in batches using known RIDs
		if err := insertAllEdges(edgePairs, vertexRIDMap); err != nil {
			log.Fatalf("Failed to insert edges: %v", err)
		}

		elapsedInsertEdges := time.Since(startInsertEdges)

		fmt.Println("Data import completed successfully!")

		elapsedImport := time.Since(startImport)
		log.Printf("Import completed in %s", elapsedImport)
		log.Printf("Load popularity: %s", elapsedLoadPopularity)
		log.Printf("Load taxonomy: %s", elapsedLoadTaxonomy)
		log.Printf("Merge vertices: %s", elapsedMerge)
		log.Printf("Insert vertices: %s", elapsedInsertVertices)
		log.Printf("Fetch vertex RIDs: %s", elapsedFetchVertexRIDs)
		log.Printf("Insert edges: %s", elapsedInsertEdges)
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
}

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
	if err := runSQLCommand("CREATE PROPERTY V.name STRING"); err != nil {
		log.Printf("Warning: could not create property V.name: %v", err)
	}
	if err := runSQLCommand("CREATE INDEX V.name UNIQUE"); err != nil {
		log.Printf("Warning: could not create index on V.name: %v", err)
	}
	return nil
}

func urlEncode(s string) string {
	return strings.ReplaceAll(s, " ", "%20")
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

func mergeVertices(vertices1, vertices2 map[string]struct{}) map[string]struct{} {
	merged := make(map[string]struct{})
	for k := range vertices1 {
		merged[k] = struct{}{}
	}
	for k := range vertices2 {
		merged[k] = struct{}{}
	}
	return merged
}

// insertAllVertices inserts all vertices using batch operations
func insertAllVertices(allVertices map[string]struct{}, popularityMap map[string]int) error {
	vertexChan := make(chan map[string]interface{})
	errChan := make(chan error, workers)
	var wg sync.WaitGroup

	// Worker pool
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]BatchOperation, 0, batchSize)
			for vertex := range vertexChan {
				batch = append(batch, BatchOperation{
					Type: "c",
					Record: map[string]interface{}{
						"@class":     "V",
						"name":       vertex["name"],
						"popularity": vertex["popularity"],
					},
				})
				if len(batch) >= batchSize {
					if err := sendBatchRequest(batch, true); err != nil {
						errChan <- err
						return
					}
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				errChan <- sendBatchRequest(batch, true)
			}
		}()
	}

	// Feed data to workers
	go func() {
		for name := range allVertices {
			popularity := 0
			if p, ok := popularityMap[name]; ok {
				popularity = p
			}
			vertexChan <- map[string]interface{}{"name": name, "popularity": popularity}
		}
		close(vertexChan)
	}()

	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil

}

func insertAllEdges(edgePairs [][2]string, vertexRIDMap map[string]string) error {
	edgeChan := make(chan []BatchOperation, 10) // Buffered channel to hold edge batches
	errorChan := make(chan error, 1)            // Channel to capture errors
	doneChan := make(chan struct{})             // Channel to signal completion of processing

	// Worker function to process edge batches
	worker := func() {
		for batch := range edgeChan {
			if err := sendBatchRequest(batch, true); err != nil {
				errorChan <- err
				return
			}
		}
		doneChan <- struct{}{}
	}

	// Launch worker goroutines
	numWorkers := workers // Number of workers to process batches concurrently
	for i := 0; i < numWorkers; i++ {
		go worker()
	}

	// Prepare edge batches and send them to the channel
	go func() {
		operations := make([]BatchOperation, 0, batchSize)
		for _, pair := range edgePairs {
			fromName, toName := pair[0], pair[1]

			fromRID, okFrom := vertexRIDMap[fromName]
			if !okFrom {
				log.Printf("Warning: from vertex not found: %s", fromName)
				continue
			}

			toRID, okTo := vertexRIDMap[toName]
			if !okTo {
				log.Printf("Warning: to vertex not found: %s", toName)
				continue
			}

			op := BatchOperation{
				Type: "c",
				Record: map[string]interface{}{
					"@class": "E",
					"out":    fromRID,
					"in":     toRID,
				},
			}
			operations = append(operations, op)

			if len(operations) == batchSize {
				edgeChan <- operations // Send batch to channel
				operations = make([]BatchOperation, 0, batchSize)
			}
		}

		// Send any remaining operations
		if len(operations) > 0 {
			edgeChan <- operations
		}

		close(edgeChan) // Close the channel to signal no more batches
	}()

	// Wait for all workers to complete or an error to occur
	for i := 0; i < numWorkers; i++ {
		select {
		case <-doneChan:
			// Worker finished successfully
		case err := <-errorChan:
			// An error occurred
			return err
		}
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
