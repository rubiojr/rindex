package rindex

import (
	"io/ioutil"
	"path/filepath"

	"github.com/blugelabs/bluge/search"
)

type SearchResultVisitor = func() bool
type FieldVisitor = func(field string, value []byte) bool

func (i Indexer) readyForSearch() bool {
	files, err := ioutil.ReadDir(i.IndexPath)
	if err != nil {
		return false
	}

	for _, f := range files {
		if ok, _ := filepath.Match("*.seg", f.Name()); ok {
			return true
		}
	}

	return false
}

// Search searches the index and calls srVisitor for every result obtained and
// fVisitor for every field in that search result.
func (i *Indexer) Search(query string, fVisitor FieldVisitor, srVisitor SearchResultVisitor) (uint64, error) {
	if !i.readyForSearch() {
		return 0, ErrSearchNotReady
	}

	count := uint64(0)
	err := i.IndexEngine.Search(query, func(iter search.DocumentMatchIterator) error {
		match, err := iter.Next()
		for err == nil && match != nil {
			count++
			if fVisitor != nil {
				err = match.VisitStoredFields(fVisitor)
				if err != nil {
					return err
				}
			}
			if srVisitor != nil && !srVisitor() {
				break
			}
			match, err = iter.Next()
		}

		return err
	})

	return count, err
}
