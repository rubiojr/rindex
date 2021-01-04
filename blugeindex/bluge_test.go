package blugeindex

import (
	"os"
	"strings"
	"testing"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/rubiojr/rindex/internal/testutil"
)

func TestIndex(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	ch := make(chan Indexable)
	ich := i.Index(ch)
	ch <- Indexable{Document: doc, Path: "test"}
	close(ch)
	for range ich {

	}

	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}

	ch = make(chan Indexable)
	ich = i.Index(ch)

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename2", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test2"}
	<-ich
	doc = bluge.NewDocument("3").
		AddField(bluge.NewTextField("filename3", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test3"}
	close(ch)

	for range ich {
	}

	if count, err := i.Count(); count != 3 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
}

func TestIndexWithBuffer(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	ch := make(chan Indexable, 10)
	ich := i.Index(ch)

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test"}
	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename2", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test2"}
	doc = bluge.NewDocument("3").
		AddField(bluge.NewTextField("filename3", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test3"}
	close(ch)

	for range ich {
	}

	if count, err := i.Count(); count != 3 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
}

func TestIndexBatched(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 10)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	ch := make(chan Indexable)
	ich := i.Index(ch)
	ch <- Indexable{Document: doc, Path: "test"}
	close(ch)
	for range ich {

	}

	if count, err := i.Count(); count != 1 {
		t.Fatalf("documents found: %d (%v)", count, err)
	}

	ch = make(chan Indexable)
	ich = i.Index(ch)

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename2", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test2"}
	<-ich
	doc = bluge.NewDocument("3").
		AddField(bluge.NewTextField("filename3", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test3"}
	close(ch)

	for range ich {
	}

	if count, err := i.Count(); count != 3 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
}

func TestIndexBatchedBuffered(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 10)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	ch := make(chan Indexable, 10)
	ich := i.Index(ch)
	ch <- Indexable{Document: doc, Path: "test"}
	close(ch)
	for range ich {
	}

	if count, err := i.Count(); count != 1 {
		t.Fatalf("documents found: %d (%v)", count, err)
	}

	ch = make(chan Indexable)
	ich = i.Index(ch)

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename2", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test2"}
	<-ich
	doc = bluge.NewDocument("3").
		AddField(bluge.NewTextField("filename3", string("test")).StoreValue().HighlightMatches())
	ch <- Indexable{Document: doc, Path: "test3"}
	close(ch)

	for range ich {
	}

	if count, err := i.Count(); count != 3 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
}

func TestBlugeSearch(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches()).
		AddField(bluge.NewCompositeFieldExcluding("_all", nil))

	ch := make(chan Indexable, 10)
	ich := i.Index(ch)
	ch <- Indexable{Document: doc, Path: "test"}
	close(ch)
	for range ich {
	}

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
}

func TestFuzziness(t *testing.T) {
	os.MkdirAll("tmp", 0755)

	i := NewBlugeIndex(testutil.IndexPath(), 0)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches()).
		AddField(bluge.NewCompositeFieldExcluding("_all", nil))

	ch := make(chan Indexable, 10)
	ich := i.Index(ch)
	ch <- Indexable{Document: doc, Path: "test"}
	close(ch)
	for range ich {
	}

	err := i.Search("tes~3", func(iter search.DocumentMatchIterator) error {
		match, err := iter.Next()
		if err != nil {
			t.Error(err)
		}
		if match == nil {
			t.Error("should find a match")
		}

		return nil
	})
	if err == nil || !strings.HasPrefix(err.Error(), "fuzziness exceeds max") {
		t.Fatalf("error searching: %+v", err)
	}
}
