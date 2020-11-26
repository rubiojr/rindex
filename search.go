package rindex

type SearchResultVisitor = func() bool
type FieldVisitor = func(field string, value []byte) bool

// Search searches the index and calls srVisitor for every result obtained and
// fVisitor for every field in that search result.
func (i *Indexer) Search(query string, fVisitor FieldVisitor, srVisitor SearchResultVisitor) (uint64, error) {
	count := uint64(0)
	reader, err := i.IndexEngine.OpenReader()
	if err != nil {
		return count, err
	}
	defer reader.Close()

	iter, err := i.IndexEngine.SearchWithReaderAndQuery(query, reader)
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
