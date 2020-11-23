package rindex

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/rubiojr/rindex/blugeindex"
)

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func indexPath() string {
	return fmt.Sprintf("tmp/%d/test.idx", rand.Intn(100000))
}

func TestMain(m *testing.M) {
	os.Setenv("RESTIC_REPOSITORY", "tmp/repo")
	os.Setenv("RESTIC_PASSWORD", "test")
	os.Exit(m.Run())
}

func TestSetBatchSize(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(indexPath())
	if err != nil {
		t.Fatal(err)
	}
	idxOpts := IndexOptions{
		Filter:             "*",
		BatchSize:          10,
		RepositoryLocation: "tmp/repo",
		RepositoryPassword: "test",
	}
	_, _ = idx.Index(context.Background(), idxOpts, progress)
	if idx.IndexEngine.BatchSize != 10 {
		t.Errorf("Index function does not set indexing engine batch size. Expected 10, available %d", idx.IndexEngine.BatchSize)
	}
}

func TestIndexWithPath(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(indexPath())
	if err != nil {
		t.Fatal(err)
	}
	idxOpts := IndexOptions{
		Filter:             "*",
		RepositoryLocation: "tmp/repo",
		RepositoryPassword: "test",
	}

	stats, err := idx.Index(context.Background(), idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 3 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedNodes != 6 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedFiles != 4 {
		t.Errorf("%+v", stats)
	}
	if stats.AlreadyIndexed != 1 {
		t.Errorf("%+v", stats)
	}
	if stats.DataBlobs != 2 {
		t.Errorf("%+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}

	// reindex not enabled
	stats, err = idx.Index(context.Background(), idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	// previously scanned snapshots are ignored, so it won't scan more nodes
	if stats.ScannedNodes != 0 {
		t.Errorf("invalid number of indexed nodes %+v", stats)
	}
	if stats.IndexedNodes != 0 {
		t.Errorf("invalid number of indexed nodes %+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}

	// reindex enabled
	idxOpts.Reindex = true
	stats, err = idx.Index(context.Background(), idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 3 {
		t.Errorf("invalid number of indexed nodes %+v", stats)
	}
	if stats.Updated != 3 {
		t.Errorf("invalid number of updated nodes %+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
}

func TestIndexWithEngine(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(indexPath())
	if err != nil {
		t.Fatal(err)
	}
	idx.IndexEngine = blugeindex.NewBlugeIndex("tmp/test2.idx", 10)
	opts := IndexOptions{
		RepositoryLocation: "tmp/repo",
		RepositoryPassword: "test",
	}
	stats, err := idx.Index(context.Background(), opts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 3 {
		t.Errorf("%+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
}

func TestIndexWithUnbufferedProgress(t *testing.T) {
	progress := make(chan IndexStats)
	idx, err := New(indexPath())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 3 {
		t.Errorf("%+v", stats)
	}
}
