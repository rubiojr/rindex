package rindex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rubiojr/rindex/internal/testutil"
)

var devResticPath = filepath.Join(os.Getenv("HOME"), "restic-dev")

const shouldIndex = 93351

func benchIndex(batchSize uint) error {
	idx, err := New(testutil.IndexPath(), devResticPath, testutil.REPO_PASS)
	if err != nil {
		return err
	}

	idxOpts := IndexOptions{
		BatchSize: batchSize,
	}
	stats, err := idx.Index(context.Background(), idxOpts, nil)
	if err != nil {
		panic(err)
	}

	if stats.IndexedFiles != shouldIndex {
		emsg := fmt.Sprintf("number of indexed nodes (%d) does not match %d\n", stats.IndexedFiles, shouldIndex)
		panic(emsg)
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
