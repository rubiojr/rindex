package rindex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const devIndexPath = "/tmp/rindex-tests/rindex-dev.bluge"

var devResticPath = filepath.Join(os.Getenv("HOME"), "restic-dev")

const shouldIndex = 92181

func benchIndex(batchSize uint) error {
	os.Setenv("RESTIC_REPOSITORY", devResticPath)
	os.Setenv("RESTIC_PASSWORD", "test")
	os.RemoveAll("/tmp/rindex-tests")

	idx, err := New(devIndexPath, os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
	if err != nil {
		return err
	}

	idxOpts := IndexOptions{
		BatchSize: batchSize,
	}
	stats, err := idx.Index(context.Background(), idxOpts, nil)
	if stats.IndexedFiles != shouldIndex {
		emsg := fmt.Sprintf("WARNING: number of indexed nodes does not match: %d\n", stats.IndexedFiles)
		fmt.Fprint(os.Stderr, emsg)
	}
	return err
}

func BenchmarkIndex(b *testing.B) {
	err := benchIndex(0)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkIndexBatch100(b *testing.B) {
	err := benchIndex(100)
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
