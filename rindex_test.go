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
	idx := New(indexPath())
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
	idx := New(indexPath())
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
	idx := New(indexPath())
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
	idx := New(indexPath())
	stats, err := idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 3 {
		t.Errorf("%+v", stats)
	}
}

func TestSearch(t *testing.T) {
	idx := New(indexPath())

	idx.Index(context.Background(), DefaultIndexOptions, nil)
	idx.Close()

	c := make(chan SearchResult, 1)
	results, err := idx.Search(context.Background(), "empty", c, DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if results != 1 {
		t.Errorf("should yield only one result, got %d", results)
	}

	select {
	case res := <-c:
		if string(res["filename"]) != "empty" {
			t.Error("should return a file named empty")
		}
		if _, ok := res["blobs"]; !ok {
			t.Error("should have a blobs field")
		}
		if _, ok := res["repository_id"]; !ok {
			t.Error("should have a repository_id field")
		}
		if _, ok := res["mtime"]; !ok {
			t.Error("should have a mtime field")
		}
	default:
		t.Error("didn't get a result back")
	}
}

func TestSearchWithQuery(t *testing.T) {
	idx := New(indexPath())

	idx.Index(context.Background(), DefaultIndexOptions, nil)
	idx.Close()

	results, err := idx.Search(context.Background(), "filename:*", nil, DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if results != 3 {
		t.Errorf("should yield 3 results, got %d", results)
	}

	results, err = idx.Search(context.Background(), "filename:empty", nil, DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if results != 1 {
		t.Errorf("should yield 1 result, got %d", results)
	}
}
