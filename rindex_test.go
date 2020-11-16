package rindex

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/blugelabs/bluge"
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
	if stats.IndexedNodes != 2 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedNodes != 2 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedTrees != 1 {
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
		t.Errorf("invalid number of indexed nodes %v", stats)
	}
	if stats.ScannedNodes != 2 {
		t.Errorf("%v", stats)
	}
	if stats.ScannedTrees != 1 {
		t.Errorf("%v", stats)
	}
	if stats.AlreadyIndexed != 2 {
		t.Errorf("%v", stats)
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
	if stats.IndexedNodes != 2 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedNodes != 2 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedTrees != 1 {
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
	if stats.IndexedNodes != 2 {
		t.Errorf("%+v", stats)
	}
}

func TestSearch(t *testing.T) {
	idx := New(indexPath(), "tmp/repo", "test")

	for i := 0; i < 200; i++ {
		doc := bluge.NewDocument(fmt.Sprintf("%d", i))
		err := idx.IndexEngine.Index(doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	idx.Close()

	results, err := idx.Search(context.Background(), "_id:2", DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Error("more than one result found, should find only one")
	}

	if string(results[0]["_id"]) != "2" {
		t.Error("should have returned document with ID 2")
	}

	results, err = idx.Search(context.Background(), "*", DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}

	// Max 100 results by default
	if len(results) != 100 {
		t.Error("should return 100 results at most")
	}
}

func TestSearchComposite(t *testing.T) {
	idx := New(indexPath(), "tmp/repo", "test")

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "foobar").StoreValue()).
		AddField(bluge.NewTextField("stuff", "bar").StoreValue()).
		AddField(bluge.NewCompositeFieldExcluding("_all", nil))
	err := idx.IndexEngine.Index(doc)
	if err != nil {
		t.Fatal(err)
	}
	idx.IndexEngine.Close()

	results, err := idx.Search(context.Background(), "foobar", DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Error("should find only one result")
	}

	results, err = idx.Search(context.Background(), "bar", DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Error("should find only one result")
	}
	if string(results[0]["stuff"]) != "bar" {
		t.Error("invalid search result")
	}
}

func TestSearchMaxResults(t *testing.T) {
	idx := New(indexPath(), "tmp/repo", "test")

	for i := 0; i < 200; i++ {
		doc := bluge.NewDocument(fmt.Sprintf("%d", i))
		err := idx.IndexEngine.Index(doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	idx.IndexEngine.Close()

	results, err := idx.Search(context.Background(), "*", SearchOptions{MaxResults: 0})
	if err != nil {
		t.Fatal(err)
	}

	// Max 100 results by default
	if len(results) != 100 {
		t.Error("should return 100 results at most")
	}

	// MaxResults can be changed
	results, err = idx.Search(context.Background(), "*", SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 10 {
		t.Error("should return 100 results at most")
	}
}
