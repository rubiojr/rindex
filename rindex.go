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

type IndexOptions struct {
	RepositoryLocation string
	RepositoryPassword string
	IndexPath          string
	Filter             string
	BatchSize          int
	IndexEngine        *blugeindex.BlugeIndex
	AppendFileMeta     bool
}

func Index(opts *IndexOptions, progress chan IndexStats) (IndexStats, error) {
	indexer := NewFileIndexer()
	return IndexWithIndexer(opts, indexer, progress)
}

func NewIndexOptions(repoLocation, repoPassword, indexPath, filter string) *IndexOptions {
	return &IndexOptions{
		RepositoryLocation: repoLocation,
		RepositoryPassword: repoPassword,
		IndexPath:          indexPath,
		IndexEngine:        blugeindex.NewBlugeIndex(indexPath, 0),
		Filter:             filter,
		AppendFileMeta:     true,
	}
}

func IndexWithIndexer(opts *IndexOptions, indexer Indexer, progress chan IndexStats) (IndexStats, error) {
	var bindex *blugeindex.BlugeIndex
	if opts.IndexEngine != nil {
		bindex = opts.IndexEngine
	} else if opts.IndexPath != "" {
		bindex = blugeindex.NewBlugeIndex(opts.IndexPath, opts.BatchSize)
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
			select {
			case progress <- stats:
			default:
			}
			if node.Type != "file" {
				stats.NonFileNodes++
				continue
			}
			fileID := fmt.Sprintf("%x", nodeFileID(node))
			if match, err := bindex.Get(fileID); match != nil {
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
			if doc, ok := indexer.ShouldIndex(fileID, bindex, node, repo); ok {
				if opts.AppendFileMeta {
					doc.AddField(bluge.NewTextField("filename", string(node.Name)).StoreValue().HighlightMatches()).
						AddField(bluge.NewTextField("repository_location", repo.Backend().Location()).StoreValue().HighlightMatches()).
						AddField(bluge.NewTextField("repository_id", repo.Config().ID).StoreValue().HighlightMatches()).
						AddField(bluge.NewDateTimeField("mod_time", node.ModTime).StoreValue().HighlightMatches()).
						AddField(bluge.NewTextField("blobs", marshalBlobIDs(node.Content)).StoreValue())
				}
				err = bindex.Index(doc)
				if err != nil {
					stats.Errors = append(stats.Errors, err)
				} else {
					stats.IndexedNodes++
				}
			}
		}
	}

	return stats, bindex.Close()
}

func nodeFileID(node *restic.Node) [32]byte {
	var bb []byte
	for _, c := range node.Content {
		bb = append(bb, []byte(c[:])...)
	}
	return sha256.Sum256(bb)
}

func marshalBlobIDs(ids restic.IDs) string {
	j, err := json.Marshal(ids)
	if err != nil {
		panic(err)
	}
	return string(j)
}
