package blugeindex

import (
	"testing"

	"github.com/blugelabs/bluge"
)

func TestBlugeIndex(t *testing.T) {
	i := NewBlugeIndex("tmp/test.idx", 0)
	defer i.Close()

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}
	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	// Test the writer will be automatically re-opened
	err = i.Index(doc)
	if err != nil {
		t.Error(err)
	}
	if count, err := i.Count(); count != 2 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
}

func TestBlugeGet(t *testing.T) {
	i := NewBlugeIndex("tmp/testblugeget.idx", 0)
	i.Close()

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())
	err := i.Index(doc)

	match, err := i.Get("1")
	if err != nil {
		t.Error(err)
	}
	if match == nil {
		t.Error("should find a match")
	}

	match, err = i.Get("5")
	if err != nil {
		t.Error(err)
	}
	if match != nil {
		t.Error("should not find a match")
	}
}

func TestBatchedWrites(t *testing.T) {
	i := NewBlugeIndex("tmp/testbatched.idx", 3)
	defer i.Close()

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches())
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}

	doc2 := bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", "test2").StoreValue().HighlightMatches())
	err = i.Index(doc2)
	if err != nil {
		t.Error(err)
	}

	if i.docsBatched != 2 {
		t.Error("doc wasn't batched")
	}

	if count, err := i.Count(); count != 0 {
		t.Errorf("should have no documents in the index, found %d. %v", count, err)
	}

	err = i.Close()
	if err != nil {
		t.Error(err)
	}

	if i.IsDirty() {
		t.Error("should not be dirty after closing")
	}

	if count, err := i.Count(); count != 2 {
		t.Errorf("should have two documents in the index, found %d. %v", count, err)
	}
}

func TestBlugeSearch(t *testing.T) {
	i := NewBlugeIndex("tmp/testsearch.idx", 0)
	defer i.Close()
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches())
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}

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
		t.Error(err)
	}
}

func TestCaseSensitivitySearch(t *testing.T) {
	i := NewBlugeIndex("tmp/testcasesearch.idx", 0)
	defer i.Close()
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "foobar").StoreValue().HighlightMatches())
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}
	// case insensitive search
	iter, err := i.Search("filename:foobar")
	if err != nil {
		t.Error(err)
	}
	match, err := iter.Next()
	if err != nil {
		t.Error(err)
	}
	if match == nil {
		t.Error("should find a match")
	}

	// case sensitive search
	iter, err = i.Search("filename:Foobar")
	if err != nil {
		t.Error(err)
	}
	match, err = iter.Next()
	if err != nil {
		t.Error(err)
	}
	if match == nil {
		t.Error("should find a match")
	}
}

func TestInvalidSearch(t *testing.T) {
	i := NewBlugeIndex("tmp/testinvalidsearch.idx", 0)
	defer i.Close()
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches())
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}

	iter, err := i.Search("filename:dunno")
	if err != nil {
		t.Error(err)
	}
	match, err := iter.Next()
	if err != nil {
		t.Error(err)
	}
	if match != nil {
		t.Error("should not find a match")
	}
}

func TestIDSearch(t *testing.T) {
	i := NewBlugeIndex("tmp/testidsearch.idx", 0)
	defer i.Close()
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches())
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}

	reader, err := i.OpenReader()
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()

	iter, err := i.SearchWithReader("filename:test", reader)
	if err != nil {
		t.Error(err)
	}
	match, err := iter.Next()
	if err != nil {
		t.Error(err)
	}
	if match == nil {
		t.Error("should find a match")
	}
}

func TestSearchWithReader(t *testing.T) {
	i := NewBlugeIndex("tmp/searchwithreader.idx", 0)
	defer i.Close()
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", "test").StoreValue().HighlightMatches())
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}

	iter, err := i.Search("_id:1")
	if err != nil {
		t.Error(err)
	}
	match, err := iter.Next()
	if err != nil {
		t.Error(err)
	}
	if match == nil {
		t.Error("should find a match")
	}
}
