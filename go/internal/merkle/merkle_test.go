package merkle

import (
	"bytes"
	"testing"
)

func TestRootUsesOneKiBChunksAndIsDeterministic(t *testing.T) {
	data := bytes.Repeat([]byte{0xA5}, ChunkSize+1)

	first, err := Root(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Root() error = %v", err)
	}
	second, err := Root(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Root() second call error = %v", err)
	}
	if first.Size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", first.Size, len(data))
	}
	if first.ChunkCount != 2 {
		t.Fatalf("ChunkCount = %d, want 2", first.ChunkCount)
	}
	if len(first.Root) == 0 {
		t.Fatal("Root is empty")
	}
	if !bytes.Equal(first.Root, second.Root) {
		t.Fatal("Root is not deterministic")
	}
}

func TestRootRejectsEmptyInput(t *testing.T) {
	if _, err := Root(bytes.NewReader(nil)); err == nil {
		t.Fatal("Root() error = nil, want an error for empty input")
	}
}
