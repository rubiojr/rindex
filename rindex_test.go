package rindex

import (
	"context"
	"os"
	"testing"

	"github.com/rubiojr/rindex/blugeindex"
	"github.com/rubiojr/rindex/internal/testutil"
)

const FILES_TO_INDEX = 4
const SCANNED_FILES = 4

func TestMain(m *testing.M) {
	testutil.SetupRepo()
	os.Exit(m.Run())
}

func TestNew(t *testing.T) {
	// make sure rindex doesn't use restic environment variables
	os.Setenv("RESTIC_REPOSITORY", "/dev/foorepo")
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}
	_, err = idx.Index(context.Background(), DefaultIndexOptions, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetBatchSize(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}
	idxOpts := DefaultIndexOptions
	idxOpts.BatchSize = 10
	_, _ = idx.Index(context.Background(), idxOpts, progress)
	if idx.IndexEngine.BatchSize != 10 {
		t.Errorf("Index function does not set indexing engine batch size. Expected 10, available %d", idx.IndexEngine.BatchSize)
	}
}

func TestIndexWithPath(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}
	idxOpts := DefaultIndexOptions

	stats, err := idx.Index(context.Background(), idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedFiles != FILES_TO_INDEX {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedSnapshots != 1 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedNodes != 6 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedFiles != SCANNED_FILES {
		t.Errorf("%+v", stats)
	}
	if stats.AlreadyIndexed != 0 {
		t.Errorf("%+v", stats)
	}
	if stats.MissingSnapshots != 1 {
		t.Error("only one snapshot should be present")
	}
	if len(stats.Errors) != 0 {
		t.Errorf("errors found while indexing: %+v", stats.Errors)
	}
	for k := range stats.SnapshotFiles {
		if stats.SnapshotFiles[k] != 4 {
			t.Errorf("snapshot should have 4 files only: %+v", stats.SnapshotFiles)
		}
	}
	if stats.CurrentSnapshotFiles != 4 {
		t.Errorf("current snapshot files should be 4, found %d", stats.CurrentSnapshotFiles)
	}
	if stats.CurrentSnapshotTotalFiles != 4 {
		t.Errorf("current snapshot total files should be 4, found %d", stats.CurrentSnapshotTotalFiles)
	}

	// reindex not enabled
	stats, err = idx.Index(context.Background(), idxOpts, progress)
	if err != nil {
		t.Error(err)
	}
	// previously scanned snapshots are ignored, so it won't scan more nodes
	if stats.ScannedNodes != 0 {
		t.Errorf("invalid number of scanned nodes %+v", stats)
	}
	if stats.ScannedSnapshots != 0 {
		t.Errorf("invalid number of indexed snapshots %+v", stats)
	}
	if stats.IndexedFiles != 0 {
		t.Errorf("invalid number of indexed files %+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
}

func TestReindex(t *testing.T) {
	repoPath := testutil.RandomRepoURI()
	testutil.RepoWithPathPass(repoPath, testutil.REPO_PASS)

	idx, err := New(testutil.IndexPath(), repoPath, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}
	idxOpts := DefaultIndexOptions

	stats, err := idx.Index(context.Background(), idxOpts, nil)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedFiles != FILES_TO_INDEX {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedSnapshots != 1 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedNodes != 6 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedFiles != SCANNED_FILES {
		t.Errorf("%+v", stats)
	}
	if stats.AlreadyIndexed != 0 {
		t.Errorf("%+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}

	testutil.CreateSnapshotForRepo(repoPath, testutil.REPO_PASS)
	stats, err = idx.Index(context.Background(), idxOpts, nil)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedFiles != 0 {
		t.Errorf("%+v", stats)
	}
	if stats.AlreadyIndexed != FILES_TO_INDEX {
		t.Errorf("%+v", stats)
	}

	// reindex enabled
	testutil.CreateSnapshotForRepo(repoPath, testutil.REPO_PASS)
	idxOpts.Reindex = true
	stats, err = idx.Index(context.Background(), idxOpts, nil)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedFiles != FILES_TO_INDEX*3 {
		t.Errorf("invalid number of indexed files %+v", stats)
	}
	if stats.ScannedFiles != SCANNED_FILES*3 {
		t.Errorf("%+v", stats)
	}
	if stats.ScannedSnapshots != 3 {
		t.Errorf("invalid number of indexed snapshots %+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
}

func TestIndexWithEngine(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}

	idx.IndexEngine, err = blugeindex.NewBlugeIndex("tmp/test2.idx", 10)
	if err != nil {
		t.Fatal(err)
	}

	opts := DefaultIndexOptions
	stats, err := idx.Index(context.Background(), opts, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedFiles != FILES_TO_INDEX {
		t.Errorf("%+v", stats)
	}
	if len(stats.Errors) != 0 {
		t.Error("errors found while indexing")
	}
}

func TestIndexWithUnbufferedProgress(t *testing.T) {
	progress := make(chan IndexStats)
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}
	if stats.IndexedFiles != FILES_TO_INDEX {
		t.Errorf("%+v", stats)
	}
}

func TestMissingSnapshots(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}
	idxOpts := DefaultIndexOptions

	missing, err := idx.MissingSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 {
		t.Errorf("should report a missing snapshot: %+v", missing)
	}

	_, err = idx.Index(context.Background(), idxOpts, progress)
	if err != nil {
		t.Fatal(err)
	}

	missing, err = idx.MissingSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Error("should not return missing snapshots")
	}
}
