package importer

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"strconv"
)

func LoadPopularity(filePath string) (map[string]int, map[string]struct{}) {
	popularityMap := make(map[string]int)
	allVertices := make(map[string]struct{})

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open popularity file: %v", err)
	}
	defer file.Close()

	sc := bufio.NewScanner(file)
	buf := make([]byte, 0, maxScanBufferSize)
	sc.Buffer(buf, maxScanBufferSize)

	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		parts := bytes.SplitN(line, []byte{','}, 2)
		if len(parts) != 2 {
			log.Printf("Skipping invalid line in popularity file: %s", line)
			continue
		}

		name := string(bytes.Trim(parts[0], `"`))
		popularity, err := strconv.Atoi(string(parts[1]))
		if err != nil {
			log.Printf("Skipping line due to invalid popularity value: %s", line)
			continue
		}
		popularityMap[name] = popularity
		allVertices[name] = struct{}{}
	}

	if err := sc.Err(); err != nil {
		log.Fatalf("Error reading popularity file: %v", err)
	}

	return popularityMap, allVertices
}
