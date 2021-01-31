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
	// Snapshots that will be scanned
	MissingSnapshots uint64

	// List of files in every snapshot
	SnapshotFiles map[string]uint64

	// Number of files scanned in the current snapshot
	CurrentSnapshotFiles uint64
	// Total number of files in the snapshot being scanned
	CurrentSnapshotTotalFiles uint64

	// Total number of snapshots
	TotalSnapshots uint64

	m *sync.Mutex
}

func NewStats() IndexStats {
	return IndexStats{Errors: []error{}, SnapshotFiles: map[string]uint64{}, m: &sync.Mutex{}}
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

func (s *IndexStats) SetMissingSnapshots(count uint64) {
	s.m.Lock()
	s.MissingSnapshots = count
	s.m.Unlock()
}

func (s *IndexStats) SetSnapshotFiles(id string, count uint64) {
	s.m.Lock()
	s.SnapshotFiles[id] = count
	s.m.Unlock()
}

func (s *IndexStats) SetCurrentSnapshotTotalFiles(count uint64) {
	s.m.Lock()
	s.CurrentSnapshotTotalFiles = count
	s.m.Unlock()
}

func (s *IndexStats) CurrentSnapshotFilesInc() {
	s.m.Lock()
	s.CurrentSnapshotFiles++
	s.m.Unlock()
}

func (s *IndexStats) SetTotalSnapshots(count uint64) {
	s.m.Lock()
	s.TotalSnapshots = count
	s.m.Unlock()
}
