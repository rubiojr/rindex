package blugeindex

import (
	"fmt"
	"os"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/rubiojr/rindex/internal/testutil"
	"github.com/syndtr/goleveldb/leveldb"
)

func TestBlugeIndex(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())
	i.Index(doc, "test")

	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	// Test the writer will be automatically re-opened
	i.Index(doc, "test")
	if count, err := i.Count(); count != 2 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
	i.Close()

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	// Test the writer will be automatically re-opened
	err := i.Index(doc, "test")
	if err == nil || err != ErrIndexClosed {
		t.Error("shouldn't be able to write when closed")
	}
}

func TestBatchedWrites(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 3)

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches())
	i.Index(doc, "test")

	_, err := i.PathsFor("1")
	if err != leveldb.ErrNotFound {
		t.Fatal("doc should not be found in the ID database")
	}

	paths, err := i.BatchedPathsFor("1")
	if err != nil {
		t.Fatal(err)
	}

	if len(paths) != 1 {
		t.Error("should have one path only, found ", paths)
	}

	if paths[0] != "test" {
		t.Error("batched path should be test, found ", paths)
	}

	doc2 := bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", "test2").StoreValue().HighlightMatches())
	i.Index(doc2, "test2")

	if count, err := i.Count(); count != 0 {
		t.Errorf("should have two documents in the index, found %d. %v", count, err)
	}

	doc3 := bluge.NewDocument("3").
		AddField(bluge.NewTextField("filename", "test3").StoreValue().HighlightMatches())
	i.Index(doc3, "test3")

	if count, err := i.Count(); count != 3 {
		t.Errorf("should have two documents in the index, found %d. %v", count, err)
	}

	i.Close()
}

func TestBlugeSearch(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches()).
		AddField(bluge.NewCompositeFieldExcluding("_all", nil))

	i.Index(doc, "test")

	err := i.Search("filename:test", func(iter search.DocumentMatchIterator) error {
		match, err := iter.Next()
		if err != nil {
			t.Error(err)
		}
		if match == nil {
			t.Error("should find a match")
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", "Foobar").StoreValue())
	err = i.Index(doc, "Foobar")
	if err != nil {
		t.Fatal(err)
	}
	i.Close()
}

func TestPathsFor(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	defer i.Close()
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())
	i.Index(doc, "test")

	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}

	doc = bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	i.Index(doc, "test2")
	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}

	paths, err := i.PathsFor("1")
	if err != nil {
		t.Fatal(err)
	}

	if len(paths) != 2 {
		t.Fatal("should return two paths")
	}

	if paths[0] != "test" {
		t.Error("first path should match test")
	}

	if paths[1] != "test2" {
		t.Error("first path should match test2")
	}
}

func TestMaxPaths(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)

	for c := 0; c < maxPaths+10; c++ {
		doc := bluge.NewDocument("1").
			AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())
		i.Index(doc, fmt.Sprintf("test%d", c))
	}

	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}

	paths, err := i.PathsFor("1")
	if err != nil {
		t.Fatal(err)
	}

	if len(paths) != 10 {
		t.Fatal("should return two paths")
	}

	if paths[0] != "test0" {
		t.Error("first path should match test")
	}

	if paths[9] != "test9" {
		t.Error("first path should match test2")
	}
}
