package rindex

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/rubiojr/rindex/internal/testutil"
)

func TestSearch(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(testutil.IndexPath(), os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}
	count, err := idx.Search("filename:empty", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("should return two results, got %d", count)
	}

	count, err = idx.Search("_id:320b5d12843eb4a96a283a1df0a011f532dd00c921913f9e64ff25477ba1af13", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("should return two results, got %d", count)
	}
}

func TestSearchAll(t *testing.T) {
	// file IDs from the testdata backup
	idset := []string{
		"320b5d12843eb4a96a283a1df0a011f532dd00c921913f9e64ff25477ba1af13",
		"f22f05e5d1d07ab02a1c25f89d37b882855823257377313364351b9d2ca1cd22",
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
	}

	idx, err := New(testutil.IndexPath(), os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.Index(context.Background(), DefaultIndexOptions, nil)
	if err != nil {
		t.Fatal(err)
	}

	found := map[string]bool{}
	visitor := func(field string, value []byte) bool {
		if field == "_id" {
			found[string(value)] = true
		}
		return true
	}
	count, err := idx.Search("*", visitor, nil)
	if err != nil {
		t.Error(err)
	}
	if count != 3 {
		t.Errorf("should return two results, got %d", count)
	}

	for _, id := range idset {
		if _, ok := found[id]; !ok {
			t.Errorf("did not find id %s", id)
		}
	}
}

func TestSearchMultiplePaths(t *testing.T) {
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := idx.Index(context.Background(), DefaultIndexOptions, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.IndexedFiles == 0 {
		t.Fatal("should have indexed something")
	}

	var altPaths []string
	visitor := func(field string, value []byte) bool {
		if field == "alt_paths" {
			json.Unmarshal(value, &altPaths)
		}
		return true
	}

	count, err := idx.Search("dupebar", visitor, nil)
	if err != nil {
		t.Error(err)
	}

	if count != 1 {
		t.Fatalf("should return one result, got %d", count)
	}

	pFound := map[string]bool{}
	for _, path := range altPaths {
		pFound[path] = true
	}

	for _, p := range []string{"/testdata/bar", "/testdata/foo/dupebar"} {
		if _, ok := pFound[p]; !ok {
			t.Errorf("path %s not found in alt_paths", p)
		}
	}
}
