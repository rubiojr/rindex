package blugeindex

import (
	"testing"

	"github.com/blugelabs/bluge"
)

func TestBlugeIndex(t *testing.T) {
	Init("testdata/test.idx", 1)
	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	i := BlugeInstance()
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}
	if count, err := i.Count(); count != 1 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
	err = i.Close()
	if err != nil {
		t.Error(err)
	}

	doc = bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())

	// Test the writer will be automatically re-opened
	err = i.Index(doc)
	if err != nil {
		t.Error(err)
	}
	err = i.Close()
	if err != nil {
		t.Error(err)
	}
	if count, err := i.Count(); count != 2 {
		t.Errorf("documents found: %d (%v)", count, err)
	}
}

func TestBlugeGet(t *testing.T) {
	i := BlugeInstance()
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
	i := NewBlugeIndex("testdata/testbatched.idx", 3)

	doc := bluge.NewDocument("1").
		AddField(bluge.NewTextField("filename", string("test")).StoreValue().HighlightMatches())
	err := i.Index(doc)
	if err != nil {
		t.Error(err)
	}

	doc2 := bluge.NewDocument("2").
		AddField(bluge.NewTextField("filename", string("test2")).StoreValue().HighlightMatches())
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
