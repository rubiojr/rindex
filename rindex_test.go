package rindex

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rindex/blugeindex"
)

func TestMain(m *testing.M) {
	os.Setenv("RESTIC_REPOSITORY", "tmp/repo")
	os.Setenv("RESTIC_PASSWORD", "test")
	os.Exit(m.Run())
}

func TestIndexWithPath(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idxOpts := &IndexOptions{
		RepositoryLocation: "tmp/repo",
		RepositoryPassword: "test",
		Filter:             "*",
		IndexPath:          "tmp/test.idx",
		AppendFileMeta:     true,
	}

	stats, err := Index(idxOpts, progress)
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
	stats, err = Index(idxOpts, progress)
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
	idxOpts := &IndexOptions{
		RepositoryLocation: "tmp/repo",
		RepositoryPassword: "test",
		Filter:             "*",
		IndexEngine:        blugeindex.NewBlugeIndex("tmp/test2.idx", 10),
	}
	stats, err := Index(idxOpts, progress)
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

func TestIndexWithDefaultOptions(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idxOpts := NewIndexOptions(
		"tmp/repo",
		"test",
		"tmp/testwithdefopts.idx",
	)
	stats, err := Index(idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 2 {
		t.Errorf("%+v", stats)
	}
}

func TestIndexWithUnbufferedProgress(t *testing.T) {
	progress := make(chan IndexStats)
	idxOpts := NewIndexOptions(
		"tmp/repo",
		"test",
		"tmp/testwithunbuf.idx",
	)
	stats, err := Index(idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 2 {
		t.Errorf("%+v", stats)
	}
}

func TestSearch(t *testing.T) {
	idx := blugeindex.NewBlugeIndex("tmp/testsearch.idx", 0)
	defer idx.Close()

	for i := 0; i < 200; i++ {
		doc := bluge.NewDocument(fmt.Sprintf("%d", i))
		err := idx.Index(doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := Search(context.Background(), "tmp/testsearch.idx", "_id:2", DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Error("more than one result found, should find only one")
	}

	if string(results[0]["_id"]) != "2" {
		t.Error("should have returned document with ID 2")
	}

	results, err = Search(context.Background(), "tmp/testsearch.idx", "*", DefaultSearchOptions)
	if err != nil {
		t.Fatal(err)
	}

	// Max 100 results by default
	if len(results) != 100 {
		t.Error("should return 100 results at most")
	}
}

func TestSearchMaxResults(t *testing.T) {
	idx := blugeindex.NewBlugeIndex("tmp/testsearchmax.idx", 0)
	defer idx.Close()

	for i := 0; i < 200; i++ {
		doc := bluge.NewDocument(fmt.Sprintf("%d", i))
		err := idx.Index(doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := Search(context.Background(), "tmp/testsearch.idx", "*", &SearchOptions{MaxResults: 0})
	if err != nil {
		t.Fatal(err)
	}

	// Max 100 results by default
	if len(results) != 100 {
		t.Error("should return 100 results at most")
	}

	// MaxResults can be changed
	results, err = Search(context.Background(), "tmp/testsearch.idx", "*", &SearchOptions{MaxResults: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 10 {
		t.Error("should return 100 results at most")
	}
}
