package rindex

// Filter determines if a file should be indexed or not
type Filter interface {
	ShouldIndex(path string) bool
}

// MatchAllFilter will index all files found
type MatchAllFilter struct{}

// ShouldIndex returns true for every file foud
func (m *MatchAllFilter) ShouldIndex(path string) bool {
	return true
}
