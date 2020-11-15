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

// Indexer is the interface custom indexers should implement
type Indexer interface {
	ShouldIndex(string, *blugeindex.BlugeIndex, *restic.Node, *repository.Repository) (*bluge.Document, bool)
}

// IndexStats is returned every time an new document is indexed or when
// the indexing process finishes.
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

// IndexOptions to be passed to Index
type IndexOptions struct {
	RepositoryLocation string
	RepositoryPassword string
	IndexPath          string
	Filter             string
	BatchSize          int
	IndexEngine        *blugeindex.BlugeIndex
	AppendFileMeta     bool
	Indexer            Indexer
}

// SearchResult is returned for every search hit during a search process
type SearchResult map[string][]byte

// SearchOptions to be passed to the Search function
type SearchOptions struct {
	MaxResults int64
}

const searchDefaultMaxResults = 100

func (opts *SearchOptions) setDefaults() {
	if opts.MaxResults == 0 {
		opts.MaxResults = searchDefaultMaxResults
	}
}

var DefaultSearchOptions = &SearchOptions{
	MaxResults: searchDefaultMaxResults,
}

func (opts *IndexOptions) setDefaults() {
	if opts.Indexer == nil {
		opts.Indexer = NewFileIndexer()
	}
	if opts.Filter == "" {
		opts.Filter = "*"
	}
	if opts.IndexEngine == nil {
		opts.IndexEngine = blugeindex.NewBlugeIndex(opts.IndexPath, opts.BatchSize)
	}
}

func NewIndexOptions(repoLocation, repoPassword, indexPath string) *IndexOptions {
	return &IndexOptions{
		RepositoryLocation: repoLocation,
		RepositoryPassword: repoPassword,
		IndexPath:          indexPath,
		IndexEngine:        blugeindex.NewBlugeIndex(indexPath, 0),
		Filter:             "*",
		AppendFileMeta:     true,
		Indexer:            NewFileIndexer(),
	}
}

func Index(opts *IndexOptions, progress chan IndexStats) (IndexStats, error) {
	if opts.IndexPath == "" && opts.IndexEngine == nil {
		return IndexStats{}, errors.New("missing IndexPath")
	}
	opts.setDefaults()

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
			if match, err := opts.IndexEngine.Get(fileID); match != nil {
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

			if doc, ok := opts.Indexer.ShouldIndex(fileID, opts.IndexEngine, node, repo); ok {
				if opts.AppendFileMeta {
					doc.AddField(bluge.NewTextField("filename", string(node.Name)).StoreValue()).
						AddField(bluge.NewTextField("repository_location", repo.Backend().Location()).StoreValue()).
						AddField(bluge.NewTextField("repository_id", repo.Config().ID).StoreValue()).
						AddField(bluge.NewDateTimeField("mod_time", node.ModTime).StoreValue()).
						AddField(bluge.NewTextField("blobs", marshalBlobIDs(node.Content)).StoreValue()).
						AddField(bluge.NewCompositeFieldExcluding("_all", nil))
				}
				err = opts.IndexEngine.Index(doc)
				if err != nil {
					stats.Errors = append(stats.Errors, err)
				} else {
					stats.IndexedNodes++
				}
			}
		}
	}

	return stats, opts.IndexEngine.Close()
}

func Search(ctx context.Context, indexPath string, query string, opts *SearchOptions) ([]SearchResult, error) {
	opts.setDefaults()

	idx := blugeindex.NewBlugeIndex(indexPath, 0)

	reader, err := idx.OpenReader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	iter, err := idx.SearchWithReader(query, reader)
	if err != nil {
		return nil, err
	}

	results := []SearchResult{}
	match, err := iter.Next()
	count := int64(0)
	for err == nil && match != nil {
		if count >= opts.MaxResults {
			break
		}
		result := SearchResult{}
		err = match.VisitStoredFields(func(field string, value []byte) bool {
			result[field] = value
			return true
		})
		if err == nil && len(result) > 0 {
			results = append(results, result)
		}
		count++
		match, err = iter.Next()
	}

	return results, err
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
