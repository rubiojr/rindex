package blugeindex

import (
	"os"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/rubiojr/rindex/internal/testutil"
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
