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
	"time"

	"github.com/spf13/cobra"
)

const (
	orientDBBaseURL  = "http://localhost:2480" // Replace with your OrientDB REST endpoint
	orientDBUsername = "root"
	orientDBPassword = "rootpwd"
	databaseName     = "dbcli"
	batchSize        = 25000 // Adjust batch size as needed
)

var importCmd = &cobra.Command{
	Use:   "import [data directory]",
	Short: "Import data from popularity and taxonomy files into OrientDB using script batches",
	Long: `Import vertices and edges from the given data directory into OrientDB.
This version uses "type":"script" operations, splitting into batches, avoiding re-creating vertices,
and not using transactions.
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

		startImport := time.Now()

		// Load popularity
		popularityMap := loadPopularity(filepath.Join(dataDir, "popularity_iw.csv"))

		// Load all edges
		edges := loadAllEdges(filepath.Join(dataDir, "taxonomy_iw.csv"))

		// We'll need to send multiple batches if edges > batchSize
		var vertexRIDMap = make(map[string]string) // vertexName -> RID

		for i := 0; i < len(edges); i += batchSize {
			end := i + batchSize
			if end > len(edges) {
				end = len(edges)
			}
			batchEdges := edges[i:end]

			// Build script for this batch
			scriptCommands, _ := buildBatchScript(batchEdges, vertexRIDMap, popularityMap)

			// Send script
			newlyCreatedVertices, err := sendScriptBatch(scriptCommands)
			if err != nil {
				log.Fatalf("Batch execution failed: %v", err)
			}

			// Update vertexRIDMap with newly created vertices' RIDs
			for name, rid := range newlyCreatedVertices {
				vertexRIDMap[name] = rid
			}

			log.Printf("Processed %d edges so far...", end)
		}

		fmt.Println("Data import completed successfully!")
		log.Printf("Total import time: %s", time.Since(startImport))
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
	if err := runSQLCommand("CREATE PROPERTY V.name STRING"); err != nil {
		log.Printf("Warning: could not create property V.name: %v", err)
	}
	if err := runSQLCommand("CREATE INDEX V.name UNIQUE"); err != nil {
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

func loadPopularity(filePath string) map[string]int {
	popularityMap := make(map[string]int, 1024) // give a starting capacity if known

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open popularity file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// Find the comma
		commaIndex := bytes.IndexByte(line, ',')
		if commaIndex < 0 {
			// No comma found, skip line
			continue
		}

		// name and popularity as slices
		nameBytes := line[:commaIndex]
		popBytes := line[commaIndex+1:]

		// Trim quotes and whitespace on name if present
		nameBytes = bytes.Trim(nameBytes, `" `)

		// Convert popularity to int
		popStr := string(popBytes) // strconv.Atoi requires a string
		popularity, err := strconv.Atoi(popStr)
		if err != nil {
			// Invalid popularity value, skip line
			continue
		}

		// Insert into map
		popularityMap[string(nameBytes)] = popularity
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading popularity file: %v", err)
	}

	return popularityMap
}

func loadAllEdges(filePath string) [][2]string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open edges file: %v", err)
	}
	defer file.Close()

	var edges [][2]string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		// Check if line matches pattern: "field1","field2"
		// Steps:
		// 1. Must start with "
		if len(line) < 6 || line[0] != '"' {
			// Too short or doesn't start with quote
			continue
		}

		// Find closing quote for field1
		firstQuoteEnd := bytes.IndexByte(line[1:], '"')
		if firstQuoteEnd == -1 {
			// No closing quote for first field
			continue
		}
		firstQuoteEnd += 1 // Adjust because we started from line[1:]
		// Now line[firstQuoteEnd] == '"'

		// After first field's ending quote, we must see: ,"
		// So next two chars should be ',' and '"'
		if firstQuoteEnd+2 >= len(line) {
			// Not enough length for ',' and '"'
			continue
		}
		if line[firstQuoteEnd+1] != ',' || line[firstQuoteEnd+2] != '"' {
			// Not in the format "field1","field2"
			continue
		}

		// Find closing quote for field2
		secondFieldStart := firstQuoteEnd + 3 // position after ,"
		secondQuoteEnd := bytes.IndexByte(line[secondFieldStart:], '"')
		if secondQuoteEnd == -1 {
			// no ending quote for second field
			continue
		}
		secondQuoteEnd += secondFieldStart
		// Now line[secondQuoteEnd] == '"'

		// Check if there's no extra characters after second quote
		if secondQuoteEnd != len(line)-1 {
			// Extra chars after second field's quote
			continue
		}

		// Extract fields without quotes
		fromBytes := line[1:firstQuoteEnd]               // between first pair of quotes
		toBytes := line[secondFieldStart:secondQuoteEnd] // between second pair of quotes

		// Convert to string
		fromStr := string(fromBytes)
		toStr := string(toBytes)

		edges = append(edges, [2]string{fromStr, toStr})
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading edges file: %v", err)
	}

	return edges
}

// buildBatchScript creates a script with:
// - CREATE VERTEX statements for any new vertices not in vertexRIDMap
// - CREATE EDGE statements for the batch of edges
// - A RETURN statement to get the newly created vertices
func buildBatchScript(edges [][2]string, vertexRIDMap map[string]string, popularityMap map[string]int) ([]string, []string) {
	scriptCommands := []string{}
	vertexVarMap := make(map[string]string) // name -> varName for this batch
	var newVertices []string
	vertexCount := 0

	// Create vertices that are not known
	for _, e := range edges {
		for _, vertexName := range e {
			// Already known globally?
			if _, known := vertexRIDMap[vertexName]; known {
				continue
			}
			// Already scheduled this batch?
			if _, inBatch := vertexVarMap[vertexName]; inBatch {
				continue
			}

			safeName, ok := sanitizeName(vertexName)
			if !ok || safeName == "" {
				// Can't safely use this name in SQL, skip this vertex
				continue
			}

			varName := fmt.Sprintf("$v_%d", vertexCount)
			vertexCount++

			if pop, ok := popularityMap[vertexName]; ok {
				scriptCommands = append(scriptCommands,
					fmt.Sprintf("LET %s = CREATE VERTEX V SET name = \"%s\", popularity = %d", varName[1:], safeName, pop))
			} else {
				scriptCommands = append(scriptCommands,
					fmt.Sprintf("LET %s = CREATE VERTEX V SET name = \"%s\"", varName[1:], safeName))
			}

			vertexVarMap[vertexName] = varName
			newVertices = append(newVertices, vertexName)
		}
	}

	// Create edges
	for _, e := range edges {
		from := e[0]
		to := e[1]

		// If we couldn't create or recognize either vertex, skip this edge
		fromRef, fromOk := getVertexReference(from, vertexRIDMap, vertexVarMap)
		toRef, toOk := getVertexReference(to, vertexRIDMap, vertexVarMap)
		if !fromOk || !toOk {
			// At least one vertex is unavailable, skip this edge
			continue
		}

		scriptCommands = append(scriptCommands, fmt.Sprintf("CREATE EDGE E FROM %s TO %s", fromRef, toRef))
	}

	// Return newly created vertices as a JSON array of objects
	if len(newVertices) > 0 {
		var objectList []string
		for i := 0; i < vertexCount; i++ {
			objectList = append(objectList, fmt.Sprintf(`{"name": $v_%d.name, "@rid": $v_%d.@rid}`, i, i))
		}
		returnCmd := "RETURN [" + strings.Join(objectList, ",") + "]"
		scriptCommands = append(scriptCommands, returnCmd)
	} else {
		scriptCommands = append(scriptCommands, "RETURN []")
	}

	return scriptCommands, newVertices
}

// sanitizeName attempts to safely include a name in an SQL string by using single quotes and escaping internal single quotes.
// Returns the sanitized name and a boolean indicating success.
func sanitizeName(name string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}

	// Replace single quotes with doubled single quotes for SQL escaping.
	name = strings.ReplaceAll(name, "'", "''")

	// Additional checks can be made if needed, for example:
	// If name contains control characters or unprintable chars, skip.
	// For now, we assume this is sufficient.

	return name, true
}

// getVertexReference returns the reference (RID or variable) for a given vertex name.
// If the vertex is known globally (vertexRIDMap), return the RID.
// If it's in the current batch (vertexVarMap), return the variable.
// If not found, return false.
func getVertexReference(vertexName string, vertexRIDMap map[string]string, vertexVarMap map[string]string) (string, bool) {
	if rid, known := vertexRIDMap[vertexName]; known {
		return rid, true
	}
	if varName, inBatch := vertexVarMap[vertexName]; inBatch {
		return varName, true
	}
	return "", false
}

// sendScriptBatch sends a script request with transaction=false
// Parses the returned JSON to extract newly created vertices RIDs
func sendScriptBatch(scriptCommands []string) (map[string]string, error) {
	batch := map[string]interface{}{
		"transaction": false,
		"operations": []map[string]interface{}{
			{
				"type":     "script",
				"language": "sql",
				"script":   scriptCommands,
			},
		},
	}

	jsonData, err := json.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %w", err)
	}

	url := fmt.Sprintf("%s/batch/%s", orientDBBaseURL, databaseName)
	req, err := http.NewRequest("POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create batch request: %w", err)
	}

	req.SetBasicAuth(orientDBUsername, orientDBPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send batch: %w", err)
	}
	defer resp.Body.Close()

	// Log the raw response body for debugging
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Raw response body: %s, %s", string(body), batch)
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var result struct {
		Result []interface{} `json:"result"`
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("batch script execution failed with status: %d, body: %s, batch: %s", resp.StatusCode, string(body), batch)
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// The result should contain the return value from our RETURN command as the last command.
	// We expected it to be an array of vertices. Each vertex is a record with @rid.
	newVerticesMap := make(map[string]string)
	if len(result.Result) > 0 {
		// result.Result[0] should be what we returned (the array of new vertices)
		vertices, ok := result.Result[0].([]interface{})
		if !ok {
			// Could be empty or not what we expect
			return newVerticesMap, nil
		}
		// Each element should be a map with @rid and name
		for _, v := range vertices {
			rec, ok := v.(map[string]interface{})
			if !ok {
				continue
			}
			// Extract @rid and name
			rid, _ := rec["@rid"].(string)
			name, _ := rec["name"].(string)
			if rid != "" && name != "" {
				newVerticesMap[name] = rid
			}
		}
	}

	return newVerticesMap, nil
}
