package rindex

type SearchResultVisitor = func() bool
type FieldVisitor = func(field string, value []byte) bool

// Search searches the index and calls srVisitor for every result obtained and
// fVisitor for every field in that search result.
func (i *Indexer) Search(query string, fVisitor FieldVisitor, srVisitor SearchResultVisitor) (uint64, error) {
	count := uint64(0)
	iter, err := i.IndexEngine.Search(query)
	if err != nil {
		return count, nil
	}

	match, err := iter.Next()
	for err == nil && match != nil {
		count++
		if fVisitor != nil {
			err = match.VisitStoredFields(fVisitor)
			if err != nil {
				return count, err
			}
		}
		if srVisitor != nil && !srVisitor() {
			break
		}
		match, err = iter.Next()
	}
	return count, nil
}
