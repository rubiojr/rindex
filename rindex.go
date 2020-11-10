package rindex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rindex/blugeindex"
)

// fileID is a 256-bit hash that distinguishes unique files.
type FileID struct {
	bytes [32]byte
}

type Indexer interface {
	ShouldIndex(string, *blugeindex.BlugeIndex, *restic.Node, *repository.Repository) (*bluge.Document, bool)
}

type IndexStats struct {
	TreeBlobs      int64
	Errors         []error
	NonFileNodes   int64
	Mismatch       int64
	ScannedNodes   int64
	ScannedTrees   int64
	IndexedNodes   int64
	AlreadyIndexed int64
	LastScanned    string
	LastMatch      string
}

func NewFileID(bytes [32]byte) *FileID {
	return &FileID{bytes: bytes}
}

func (id *FileID) String() string {
	return fmt.Sprintf("%x", id.bytes)
}

func NodeFileID(node *restic.Node) *FileID {
	var bb []byte
	for _, c := range node.Content {
		bb = append(bb, []byte(c[:])...)
	}
	return NewFileID(sha256.Sum256(bb))
}

func MarshalBlobIDs(ids restic.IDs) string {
	j, err := json.Marshal(ids)
	if err != nil {
		panic(err)
	}
	return string(j)
}

type IndexOptions struct {
	RepositoryLocation string
	RepositoryPassword string
	IndexPath          string
	Filter             string
	BatchSize          int
	IndexEngine        *blugeindex.BlugeIndex
}

func Index(opts *IndexOptions, progress chan IndexStats) (IndexStats, error) {
	indexer := NewFileIndexer()
	return CustomIndex(opts, indexer, progress)
}

func CustomIndex(opts *IndexOptions, indexer Indexer, progress chan IndexStats) (IndexStats, error) {
	var bluge *blugeindex.BlugeIndex
	if opts.IndexEngine != nil {
		bluge = opts.IndexEngine
	} else if opts.IndexPath != "" {
		bluge = blugeindex.NewBlugeIndex(opts.IndexPath, opts.BatchSize)
	} else {
		return IndexStats{}, errors.New("missing IndexEngine or IndexPath")
	}

	ropts := rapi.DefaultOptions
	ropts.Password = opts.RepositoryPassword
	ropts.Repo = opts.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return IndexStats{}, err
	}

	ctx := context.Background()
	stats := IndexStats{Errors: []error{}}
	if err = repo.LoadIndex(ctx); err != nil {
		return stats, err
	}

	idx := repo.Index()
	treeBlobs := []restic.ID{}
	for blob := range idx.Each(ctx) {
		if blob.Type == restic.TreeBlob {
			treeBlobs = append(treeBlobs, blob.ID)
		}
	}

	for _, blob := range treeBlobs {
		stats.ScannedTrees++
		repo.LoadBlob(ctx, restic.TreeBlob, blob, nil)
		tree, err := repo.LoadTree(ctx, blob)
		if err != nil {
			stats.Errors = append(stats.Errors, err)
			continue
		}

		for _, node := range tree.Nodes {
			stats.ScannedNodes++
			stats.LastScanned = node.Name
			progress <- stats
			if node.Type != "file" {
				stats.NonFileNodes++
				continue
			}
			fileID := NodeFileID(node)
			if match, err := bluge.Get(fileID.String()); match != nil {
				if err != nil {
					stats.Errors = append(stats.Errors, err)
				} else {
					stats.AlreadyIndexed++
				}
				continue
			}
			match, err := filepath.Match(opts.Filter, strings.ToLower(node.Name))
			if err != nil {
				stats.Errors = append(stats.Errors, err)
				continue
			}
			if !match {
				stats.Mismatch++
				continue
			}
			stats.LastMatch = node.Name
			if doc, ok := indexer.ShouldIndex(fileID.String(), bluge, node, repo); ok {
				err = bluge.Index(doc)
				if err != nil {
					stats.Errors = append(stats.Errors, err)
				} else {
					stats.IndexedNodes++
				}
			}
		}
	}

	return stats, bluge.Close()
}
