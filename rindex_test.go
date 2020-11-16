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
	return fmt.Sprintf("tmp/test%d.idx", rand.Intn(100000))
}

func TestMain(m *testing.M) {
	os.Setenv("RESTIC_REPOSITORY", "tmp/repo")
	os.Setenv("RESTIC_PASSWORD", "test")
	os.Exit(m.Run())
}

func TestSetBatchSize(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx := New(indexPath(), "tmp/repo", "test")
	idxOpts := IndexOptions{
		Filter:    "*",
		BatchSize: 10,
	}
	_, _ = idx.Index(context.Background(), idxOpts, progress)
	if idx.IndexEngine.BatchSize != 10 {
		t.Errorf("Index function does not set indexing engine batch size. Expected 10, available %d", idx.IndexEngine.BatchSize)
	}
}

func TestIndexWithPath(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx := New(indexPath(), "tmp/repo", "test")
	idxOpts := IndexOptions{
		Filter: "*",
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
	if stats.ScannedTrees != 3 {
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

	// reindex
	stats, err = idx.Index(context.Background(), idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 0 {
		t.Errorf("invalid number of indexed nodes %+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
}

func TestIndexWithEngine(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx := New(indexPath(), "tmp/repo", "test")
	idx.IndexEngine = blugeindex.NewBlugeIndex("tmp/test2.idx", 10)

	stats, err := idx.Index(context.Background(), DefaultIndexOptions, progress)
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
	idx := New(indexPath(), "tmp/repo", "test")
	stats, err := idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 3 {
		t.Errorf("%+v", stats)
	}
}

func TestSearch(t *testing.T) {
	idx := New(indexPath(), "tmp/repo", "test")

	idx.Index(context.Background(), DefaultIndexOptions, nil)
	idx.Close()

	results, err := idx.Search(context.Background(), "empty", DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Error("should yield only one result")
	}
}

func TestSearchMaxResults(t *testing.T) {
	idx := New(indexPath(), "tmp/repo", "test")
	idx.Index(context.Background(), DefaultIndexOptions, nil)
	idx.Close()

	results, err := idx.Search(context.Background(), "bar empty", SearchOptions{MaxResults: 1})
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Errorf("should return 1 results at most, got %d", len(results))
	}

	// MaxResults can be changed
	results, err = idx.Search(context.Background(), "bar empty", SearchOptions{MaxResults: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("should return 2 results at most, got %d", len(results))
	}
}
