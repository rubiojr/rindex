package rindex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const devIndexPath = "/tmp/rindex-dev.bluge"

var devResticPath = filepath.Join(os.Getenv("HOME"), "restic-dev")

func benchIndex(batchSize uint) error {
	os.Setenv("RESTIC_REPOSITORY", devResticPath)
	os.Setenv("RESTIC_PASSWORD", "test")
	os.RemoveAll(devIndexPath)

	idx := New(devIndexPath)
	idxOpts := IndexOptions{
		Filter:             "*",
		BatchSize:          batchSize,
		RepositoryLocation: os.Getenv("RESTIC_REPOSITORY"),
		RepositoryPassword: os.Getenv("RESTIC_PASSWORD"),
	}
	stats, err := idx.Index(context.Background(), idxOpts, nil)
	fmt.Printf("Indexed nodes: %d\n", stats.IndexedNodes)
	fmt.Printf("Already indexed nodes: %d\n", stats.AlreadyIndexed)
	return err
}

func BenchmarkIndex(b *testing.B) {
	err := benchIndex(0)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkIndexBatch100(b *testing.B) {
	err := benchIndex(1000)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkIndexBatch1000(b *testing.B) {
	err := benchIndex(1000)
	if err != nil {
		b.Error(err)
	}
}
