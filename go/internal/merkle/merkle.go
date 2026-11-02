// Package merkle contains the Atlas file commitment algorithm.
package merkle

import (
	"fmt"
	"io"

	merkletree "github.com/ahm23/go-merkletree-xxh"
	"github.com/zeebo/blake3"
)

// ChunkSize is the Atlas provider chunk size in bytes.
const ChunkSize = 1024

// Result describes a file commitment.
type Result struct {
	Root       []byte
	Size       int64
	ChunkCount int
}

// Root reads r in 1 KiB chunks and creates the commitment expected by Atlas
// storage providers: BLAKE3 chunk leaves and an XXH128 Merkle tree.
func Root(r io.Reader) (*Result, error) {
	buf := make([]byte, ChunkSize)
	leaves := make([][]byte, 0, 1024)
	result := &Result{}

	for {
		n, readErr := io.ReadFull(r, buf)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("read file chunk: %w", readErr)
		}
		if n > 0 {
			sum := blake3.Sum256(buf[:n])
			leaves = append(leaves, sum[:])
			result.Size += int64(n)
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}
	if len(leaves) == 0 {
		return nil, fmt.Errorf("empty files are not supported")
	}

	tree, err := merkletree.New(&merkletree.Config{XXH128: true}, leaves)
	if err != nil {
		return nil, fmt.Errorf("build Merkle tree: %w", err)
	}
	result.Root = append([]byte(nil), tree.Root...)
	result.ChunkCount = len(leaves)
	return result, nil
}
