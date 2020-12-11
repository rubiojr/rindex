package blugeindex

import (
	"os"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rindex/internal/testutil"
)

func TestBlugeIndex(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())
	i.Index(doc)

	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	// Test the writer will be automatically re-opened
	i.Index(doc)
	if count, err := i.Count(); count != 2 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
	i.Close()

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	// Test the writer will be automatically re-opened
	err := i.Index(doc)
	if err == nil || err != ErrIndexClosed {
		t.Error("shouldn't be able to write when closed")
	}
}

func TestBatchedWrites(t *testing.T) {
	i := NewBlugeIndex(testutil.IndexPath(), 3)

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches())
	i.Index(doc)

	doc2 := bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", "test2").StoreValue().HighlightMatches())
	i.Index(doc2)

	if count, err := i.Count(); count != 0 {
		t.Errorf("should have two documents in the index, found %d. %v", count, err)
	}

	doc3 := bluge.NewDocument("3").
		AddField(bluge.NewTextField("filename", "test2").StoreValue().HighlightMatches())
	i.Index(doc3)

	if count, err := i.Count(); count != 3 {
		t.Errorf("should have two documents in the index, found %d. %v", count, err)
	}

	i.Close()
}

func TestBlugeSearch(t *testing.T) {
	i := NewBlugeIndex(testutil.IndexPath(), 0)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches()).
		AddField(bluge.NewCompositeFieldExcluding("_all", nil))

	i.Index(doc)

	iter, err := i.Search("filename:test")
	if err != nil {
		t.Fatal(err)
	}
	match, err := iter.Next()
	if err != nil {
		t.Error(err)
	}
	if match == nil {
		t.Error("should find a match")
	}

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", "Foobar").StoreValue())
	err = i.Index(doc)
	if err != nil {
		t.Fatal(err)
	}
	i.Close()
}
