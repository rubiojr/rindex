package rindex

import (
	"os"
	"testing"

	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rindex/blugeindex"
)

func TestMain(m *testing.M) {
	os.Setenv("RESTIC_REPOSITORY", "testdata/repo")
	os.Setenv("RESTIC_PASSWORD", "test")
	os.Exit(m.Run())
}

func TestIndex(t *testing.T) {
	repo, err := rapi.OpenRepository(rapi.DefaultOptions)
	if err != nil {
		panic(nil)
	}

	progress := make(chan IndexStats, 10)
	idx := blugeindex.Init("testdata/test.idx", 1)
	stats, err := Index(repo, idx, "*", progress)

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
	stats, err = Index(repo, idx, "*", progress)
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
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
	if c, err := blugeindex.BlugeInstance().Count(); err == nil {
		if c != 2 {
			t.Errorf("item count %d", c)
		}
	} else {
		t.Error(err)
	}
}
