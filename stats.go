package rindex

import "sync"

// IndexStats is returned every time an new document is indexed or when
// the indexing process finishes.
type IndexStats struct {
	// Files that did not pass the filter (not indexed)
	Mismatch uint64
	// Number of nodes (files or directories) scanned
	ScannedNodes uint64
	// Number of files indexed
	IndexedFiles uint64
	// Number of snapshots visited for indexing
	ScannedSnapshots uint64
	// Number of files previously indexed
	AlreadyIndexed uint64
	// Number of files scanned
	ScannedFiles uint64
	// Errors found while scanning or indexing files
	Errors []error
	// Last file indexed
	LastMatch string
	m         *sync.Mutex
}

func NewStats() IndexStats {
	return IndexStats{Errors: []error{}, m: &sync.Mutex{}}
}

func (s *IndexStats) ErrorsAdd(err error) {
	s.m.Lock()
	s.Errors = append(s.Errors, err)
	s.m.Unlock()
}

func (s *IndexStats) ScannedSnapshotsInc() {
	s.m.Lock()
	s.ScannedSnapshots++
	s.m.Unlock()
}

func (s *IndexStats) ScannedNodesInc() {
	s.m.Lock()
	s.ScannedNodes++
	s.m.Unlock()
}

func (s *IndexStats) IndexedFilesInc() {
	s.m.Lock()
	s.IndexedFiles++
	s.m.Unlock()
}

func (s *IndexStats) MismatchInc() {
	s.m.Lock()
	s.Mismatch++
	s.m.Unlock()
}

func (s *IndexStats) ScannedFilesInc() {
	s.m.Lock()
	s.ScannedFiles++
	s.m.Unlock()
}

func (s *IndexStats) AlreadyIndexedInc() {
	s.m.Lock()
	s.AlreadyIndexed++
	s.m.Unlock()
}
