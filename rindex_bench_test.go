package rindex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

const devIndexPath = "/tmp/rindex-tests/rindex-dev.bluge"

var devResticPath = filepath.Join(os.Getenv("HOME"), "restic-dev")

func benchIndex(batchSize uint) error {
	os.Setenv("RESTIC_REPOSITORY", devResticPath)
	os.Setenv("RESTIC_PASSWORD", "test")
	os.RemoveAll("/tmp/rindex-tests")

	idx, err := New(devIndexPath)
	if err != nil {
		return err
	}

	idxOpts := IndexOptions{
		BatchSize:          batchSize,
		RepositoryLocation: os.Getenv("RESTIC_REPOSITORY"),
		RepositoryPassword: os.Getenv("RESTIC_PASSWORD"),
	}
	stats, err := idx.Index(context.Background(), idxOpts, nil)
	if stats.IndexedNodes != 91247 {
		err = errors.New("number of indexed nodes does not match")
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
