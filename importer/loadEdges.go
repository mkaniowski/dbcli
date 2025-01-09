package importer

import (
	"bufio"
	"bytes"
	"log"
	"os"
)

const maxScanBufferSize = 10 * 1024 * 1024 // 10MB buffer size

func LoadEdges(filePath string) (map[string]struct{}, [][2]string) {
	allVertices := make(map[string]struct{})
	edgePairs := make([][2]string, 0, 10000)

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open edges file: %v", err)
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
			log.Printf("Skipping invalid line in edges file: %s", line)
			continue
		}

		from := string(bytes.Trim(parts[0], `"`))
		to := string(bytes.Trim(parts[1], `"`))

		allVertices[from] = struct{}{}
		allVertices[to] = struct{}{}
		edgePairs = append(edgePairs, [2]string{from, to})
	}

	if err := sc.Err(); err != nil {
		log.Fatalf("Error reading edges file: %v", err)
	}

	return allVertices, edgePairs
}
