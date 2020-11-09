package rindex

import (
	"os"
	"testing"

	"github.com/rubiojr/rindex/blugeindex"
)

func TestMain(m *testing.M) {
	os.Setenv("RESTIC_REPOSITORY", "testdata/repo")
	os.Setenv("RESTIC_PASSWORD", "test")
	os.Exit(m.Run())
}

func TestIndexWithPath(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idxOpts := &IndexOptions{
		RepositoryLocation: "testdata/repo",
		RepositoryPassword: "test",
		Filter:             "*",
		IndexPath:          "testdata/test.idx",
	}

	stats, err := Index(idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 2 {
		t.Errorf("%v", stats)
	}
	if stats.ScannedNodes != 2 {
		t.Errorf("%v", stats)
	}
	if stats.ScannedTrees != 1 {
		t.Errorf("%v", stats)
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
		RepositoryLocation: "testdata/repo",
		RepositoryPassword: "test",
		Filter:             "*",
		IndexEngine:        blugeindex.Init("testdata/test2.idx", 10),
	}
	stats, err := Index(idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedNodes != 2 {
		t.Errorf("%v", stats)
	}
	if stats.ScannedNodes != 2 {
		t.Errorf("%v", stats)
	}
	if stats.ScannedTrees != 1 {
		t.Errorf("%v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
}
