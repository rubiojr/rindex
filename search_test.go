package rindex

import (
	"context"
	"os"
	"testing"

	"github.com/rubiojr/rindex/internal/testutil"
)

func TestSearch(t *testing.T) {
	progress := make(chan IndexStats, 10)
	path := testutil.IndexPath()
	idx, err := New(path, os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
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
		t.Errorf("should return one results, got %d", count)
	}

	count, err = idx.Search("bhash:320b5d12843eb4a96a283a1df0a011f532dd00c921913f9e64ff25477ba1af13", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("should return two results, got %d", count)
	}
	idx.Close()

	// search without indexing first, to make sure the engine has been initialized
	idx, err = New(path, os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
	count, err = idx.Search("bhash:320b5d12843eb4a96a283a1df0a011f532dd00c921913f9e64ff25477ba1af13", nil, nil)
	if err != nil {
		t.Error(err)
	}
	if count != 1 {
		t.Errorf("should return two results, got %d", count)
	}
}

func TestSearchInvalid(t *testing.T) {
	idx, err := New(testutil.IndexPath(), os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.Search("filename:empty", nil, nil)
	if err != ErrSearchNotReady {
		t.Error(err)
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
		if field == "bhash" {
			found[string(value)] = true
		}
		return true
	}
	count, err := idx.Search("*", visitor, nil)
	if err != nil {
		t.Error(err)
	}
	if count != 4 {
		t.Errorf("should return tree results, got %d", count)
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
		if field == "path" {
			altPaths = append(altPaths, string(value))
		}
		return true
	}

	count, err := idx.Search("bhash:f22f05e5d1d07ab02a1c25f89d37b882855823257377313364351b9d2ca1cd22", visitor, nil)
	if err != nil {
		t.Error(err)
	}

	if count != 2 {
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
