package cmd

import (
	"bytes"
	"dbcli/importer"
	"dbcli/utils"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

const (
	orientDBBaseURL  = "http://orientdb:2480" // Replace with your OrientDB REST endpoint
	orientDBUsername = "root"
	orientDBPassword = "rootpwd"
	databaseName     = "dbcli"

	batchSize = 20000
	workers   = 6
)

// BatchOperation represents an operation in the batch request
type BatchOperation struct {
	Type     string                 `json:"type"`
	Language string                 `json:"language,omitempty"`
	Command  string                 `json:"command,omitempty"`
	Record   map[string]interface{} `json:"record,omitempty"`
	Script   []string               `json:"script,omitempty"`
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

		// 1) Ensure the database exists
		if err := ensureDatabaseExists(); err != nil {
			log.Fatalf("Failed to ensure database existence: %v", err)
		}

		// 2) Turn off lightweight edges
		if _, err := utils.ExecuteQuery("ALTER DATABASE CUSTOM useLightweightEdges=FALSE"); err != nil {
			log.Printf("Warning: could not alter db: %v", err)
		}

		// 3) Create Vertex class & properties via REST
		if _, err := utils.ExecuteQuery("CREATE CLASS `Vertex` EXTENDS V"); err != nil {
			log.Printf("Warning: could not alter db: %v", err)
		}
		// Create properties for Vertex class
		vertexProps := map[string]map[string]string{
			"name": {
				"propertyType": "STRING",
			},
			"popularity": {
				"propertyType": "INTEGER",
			},
		}
		if err := createProperties("Vertex", vertexProps); err != nil {
			log.Printf("Warning: could not create properties on Vertex: %v", err)
		}

		// 4) Create a unique index on Vertex.name using the command endpoint
		if err := executeSQLCommand("CREATE INDEX `Vertex.name` UNIQUE"); err != nil {
			log.Printf("Warning: could not create unique index on Vertex.name: %v", err)
		}

		// 5) Create Edge class & properties via REST
		if _, err := utils.ExecuteQuery("CREATE CLASS `Edge` EXTENDS E"); err != nil {
			log.Printf("Warning: could not alter db: %v", err)
		}
		//Create properties for Edge class
		edgeProps := map[string]map[string]string{
			"in": {
				"propertyType": "LINK",
				"linkedClass":  "Vertex",
			},
			"out": {
				"propertyType": "LINK",
				"linkedClass":  "Vertex",
			},
		}
		if err := createProperties("Edge", edgeProps); err != nil {
			log.Printf("Warning: could not create properties on Edge: %v", err)
		}

		startImport := time.Now()

		// Load popularity data
		startLoadPopularity := time.Now()
		popularityMap, popularityVertices := importer.LoadPopularity(filepath.Join(dataDir, "popularity_iw.csv"))
		elapsedLoadPopularity := time.Since(startLoadPopularity)

		// Load taxonomy edges and gather vertices
		startLoadTaxonomy := time.Now()
		taxonomyVertices, edgePairs := importer.LoadEdges(filepath.Join(dataDir, "taxonomy_iw.csv"))
		elapsedLoadTaxonomy := time.Since(startLoadTaxonomy)

		// Merge vertices
		startMerge := time.Now()
		allVertices := mergeVertices(popularityVertices, taxonomyVertices)
		elapsedMerge := time.Since(startMerge)

		// Insert all vertices in batches
		startInsertVertices := time.Now()
		if err := insertAllVertices(allVertices, popularityMap); err != nil {
			log.Fatalf("Failed to insert vertices: %v", err)
		}
		elapsedInsertVertices := time.Since(startInsertVertices)

		// Fetch RIDs after inserting vertices
		startFetchVertexRIDs := time.Now()
		vertexRIDMap, err := fetchAllVertexRIDs()
		if err != nil {
			log.Fatalf("Failed to fetch vertex RIDs: %v", err)
		}
		elapsedFetchVertexRIDs := time.Since(startFetchVertexRIDs)

		// Insert edges in batches using known RIDs
		startInsertEdges := time.Now()
		if err := insertAllEdges(edgePairs, vertexRIDMap); err != nil {
			log.Fatalf("Failed to insert edges: %v", err)
		}
		elapsedInsertEdges := time.Since(startInsertEdges)

		fmt.Println("Data import completed successfully!")
		elapsedImport := time.Since(startImport)

		// Log times
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

// --------------------------------------------------------------------------------
// REST calls to ensure DB, create classes, create properties, create index, etc.
// --------------------------------------------------------------------------------

// ensureDatabaseExists checks or creates the OrientDB database via REST
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

	// If database already exists, just return
	if resp.StatusCode == http.StatusOK {
		fmt.Println("Database already exists.")
		return nil
	}

	// Otherwise create it
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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create database, status: %d, body: %s", resp.StatusCode, string(body))
	}

	fmt.Println("Database created successfully.")
	return nil
}

// createClassIfNotExists attempts to create a new class extending another class.
// Example: createClassIfNotExists("Vertex", "V") => "CREATE CLASS Vertex EXTENDS V"
func createClassIfNotExists(className, superClass string) error {
	// According to the OrientDB REST docs:
	//   POST /class/<database>/<class-name>
	// If the class already exists, OrientDB typically returns a 409 or 500.
	//
	// There's no direct "if exists" check, so we can do a GET to see if it exists.
	// If it doesn't exist, we do a POST to create it.
	// But if you want to forcibly create, just do the POST and ignore 409 errors.

	checkURL := fmt.Sprintf("%s/class/%s/%s", orientDBBaseURL, databaseName, className)
	reqCheck, err := http.NewRequest("GET", checkURL, nil)
	if err != nil {
		return err
	}
	reqCheck.SetBasicAuth(orientDBUsername, orientDBPassword)

	respCheck, err := http.DefaultClient.Do(reqCheck)
	if err != nil {
		return err
	}
	defer respCheck.Body.Close()

	if respCheck.StatusCode == http.StatusOK {
		// Class already exists, just return
		log.Printf("Class '%s' already exists.", className)
		return nil
	}

	// If not found, create it
	createURL := fmt.Sprintf("%s/class/%s/%s", orientDBBaseURL, databaseName, className)
	// We can pass ?superClass=<name> as a query parameter, or rely on OrientDB
	// to handle creation. In older versions, we might have used a command.
	// According to OrientDB docs, you can do:
	//    POST /class/<database>/<class-name>/<super-class-name>
	// We'll pass it that way to ensure extension.
	if superClass != "" {
		createURL = createURL + "/" + superClass
	}

	reqCreate, err := http.NewRequest("POST", createURL, nil)
	if err != nil {
		return err
	}
	reqCreate.SetBasicAuth(orientDBUsername, orientDBPassword)

	respCreate, err := http.DefaultClient.Do(reqCreate)
	if err != nil {
		return err
	}
	defer respCreate.Body.Close()

	if respCreate.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(respCreate.Body)
		return fmt.Errorf("failed to create class %s, status: %d, body: %s",
			className, respCreate.StatusCode, string(body))
	}

	log.Printf("Class '%s' created successfully.", className)
	return nil
}

// createProperties allows multiple property creation in a single request via POST /property/<db>/<className> with JSON body
func createProperties(className string, props map[string]map[string]string) error {
	// Example JSON:
	// {
	//   "name": {
	//     "propertyType": "STRING"
	//   },
	//   "popularity": {
	//     "propertyType": "INTEGER"
	//   }
	// }
	propsData, err := json.Marshal(props)
	if err != nil {
		return fmt.Errorf("failed to marshal properties: %w", err)
	}

	url := fmt.Sprintf("%s/property/%s/%s", orientDBBaseURL, databaseName, className)
	req, err := http.NewRequest("POST", url, bytes.NewReader(propsData))
	if err != nil {
		return fmt.Errorf("failed to create request for properties: %w", err)
	}
	req.SetBasicAuth(orientDBUsername, orientDBPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create properties: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create properties for class %s, status: %d, body: %s",
			className, resp.StatusCode, string(body))
	}

	// success
	return nil
}

// executeSQLCommand runs an SQL command via POST /command/<database>/sql
func executeSQLCommand(sql string) error {
	url := fmt.Sprintf("%s/command/%s/sql", orientDBBaseURL, databaseName)

	// The request body for a POST command must be JSON with "command": <sql> or "command": "sql to run"
	payload := map[string]interface{}{
		"command": sql,
	}
	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal SQL command: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create command request: %w", err)
	}
	req.SetBasicAuth(orientDBUsername, orientDBPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute SQL command: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("SQL command failed, status: %d, body: %s", resp.StatusCode, string(body))
	}
	return nil
}

// --------------------------------------------------------------------------
// MISC: merges vertices, inserts them in batches, inserts edges in scripts, etc.
// --------------------------------------------------------------------------

// mergeVertices merges two sets of vertex names
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

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]BatchOperation, 0, batchSize)
			for vertex := range vertexChan {
				batch = append(batch, BatchOperation{
					Type: "c",
					Record: map[string]interface{}{
						"@class":     "Vertex",
						"name":       vertex["name"],
						"popularity": vertex["popularity"],
					},
				})
				// When we hit batchSize, send a batch request
				if len(batch) >= batchSize {
					if err := sendBatchRequest(batch, true); err != nil {
						errChan <- err
						return
					}
					batch = batch[:0]
				}
			}
			// Send final leftover
			if len(batch) > 0 {
				if err := sendBatchRequest(batch, true); err != nil {
					errChan <- err
				}
			}
		}()
	}

	// Feed data into the workers
	go func() {
		for name := range allVertices {
			popularity := 0
			if p, ok := popularityMap[name]; ok {
				popularity = p
			}
			vertexChan <- map[string]interface{}{
				"name":       name,
				"popularity": popularity,
			}
		}
		close(vertexChan)
	}()

	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

// insertAllEdges inserts edges using a single "script" operation per worker
// with up to 20,000 CREATE EDGE commands in a single BEGIN/COMMIT script block.
func insertAllEdges(edgePairs [][2]string, vertexRIDMap map[string]string) error {
	edgeChan := make(chan [2]string)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scriptLines := []string{"BEGIN;"}
			count := 0

			for pair := range edgeChan {
				fromName, toName := pair[0], pair[1]
				fromRID, okFrom := vertexRIDMap[fromName]
				toRID, okTo := vertexRIDMap[toName]
				if !okFrom || !okTo {
					// skip or log
					continue
				}

				// Accumulate "CREATE EDGE Edge FROM <rid> TO <rid>"
				scriptLines = append(scriptLines, fmt.Sprintf("CREATE EDGE `Edge` FROM %s TO %s;", fromRID, toRID))
				count++

				// If we've reached batchSize, send the script as a single operation
				if count >= 10000 {
					scriptLines = append(scriptLines, "COMMIT;")
					op := BatchOperation{
						Type:     "script",
						Language: "sql",
						Script:   scriptLines,
					}
					if err := sendBatchRequest([]BatchOperation{op}, false); err != nil {
						errChan <- err
						return
					}
					// Reset for next batch
					scriptLines = []string{"BEGIN;"}
					count = 0
				}
			}

			// Send any leftover in final partial batch
			if count > 0 {
				scriptLines = append(scriptLines, "COMMIT;")
				op := BatchOperation{
					Type:     "script",
					Language: "sql",
					Script:   scriptLines,
				}
				if err := sendBatchRequest([]BatchOperation{op}, false); err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	// Producer: feed all edges
	go func() {
		for _, pair := range edgePairs {
			edgeChan <- pair
		}
		close(edgeChan)
	}()

	// Wait for all workers
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}
	return nil
}

// fetchAllVertexRIDs returns a map of name->@rid for all Vertex records
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

	log.Printf("Fetched %d vertex RIDs", len(m))

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
