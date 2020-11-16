package rindex

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/blugelabs/bluge"
	lru "github.com/hashicorp/golang-lru"
	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rindex/blugeindex"
)

// DocumentBuilder is the interface custom indexers should implement
type DocumentBuilder interface {
	ShouldIndex(string, blugeindex.BlugeIndex, *restic.Node, *repository.Repository) (*bluge.Document, bool)
}

// IndexStats is returned every time an new document is indexed or when
// the indexing process finishes.
type IndexStats struct {
	TreeBlobs      uint64
	NonFileNodes   uint64
	Mismatch       uint64
	ScannedNodes   uint64
	ScannedTrees   uint64
	IndexedNodes   uint64
	AlreadyIndexed uint64
	DataBlobs      uint64
	Errors         []error
	LastScanned    string
	LastMatch      string
}

// IndexOptions to be passed to Index
type IndexOptions struct {
	RepositoryLocation string
	RepositoryPassword string
	Filter             string
	BatchSize          uint
	AppendFileMeta     bool
	DocumentBuilder    DocumentBuilder
}

// SearchResult is returned for every search hit during a search process
type SearchResult map[string][]byte

// SearchOptions to be passed to the Search function
type SearchOptions struct {
	MaxResults int64
}

type Indexer struct {
	IndexPath   string
	IndexEngine *blugeindex.BlugeIndex
}

const searchDefaultMaxResults = 100

var DefaultSearchOptions = SearchOptions{
	MaxResults: searchDefaultMaxResults,
}

var DefaultIndexOptions = IndexOptions{
	Filter:          "*",
	BatchSize:       1,
	AppendFileMeta:  true,
	DocumentBuilder: FileDocumentBuilder{},
}

func New(indexPath string) Indexer {
	return Indexer{
		IndexEngine: blugeindex.NewBlugeIndex(indexPath, 1),
		IndexPath:   indexPath,
	}
}

func (i Indexer) Index(ctx context.Context, opts IndexOptions, progress chan IndexStats) (IndexStats, error) {
	if opts.DocumentBuilder == nil {
		opts.DocumentBuilder = FileDocumentBuilder{}
	}
	if opts.Filter == "" {
		opts.Filter = "*"
	}
	i.IndexEngine.SetBatchSize(opts.BatchSize)

	ropts := rapi.DefaultOptions
	ropts.Password = opts.RepositoryPassword
	ropts.Repo = opts.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return IndexStats{}, err
	}

	stats := IndexStats{Errors: []error{}}
	if err = repo.LoadIndex(ctx); err != nil {
		return stats, err
	}

	idx := repo.Index()
	stats.DataBlobs = uint64(idx.Count(restic.DataBlob))
	treeBlobs := []restic.ID{}
	for blob := range idx.Each(ctx) {
		if blob.Type == restic.TreeBlob {
			treeBlobs = append(treeBlobs, blob.ID)
		}
	}

	csize := opts.BatchSize
	if csize == 0 {
		csize = 1
	}
	hcache, err := lru.New(int(csize))
	if err != nil {
		panic(err)
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

			if _, ok := hcache.Get(fileID); ok {
				continue
			}
			hcache.Add(fileID, 0)

			match, err := i.IndexEngine.Get(fileID)
			if err != nil {
				stats.Errors = append(stats.Errors, err)
				continue
			}
			if match != nil {
				stats.AlreadyIndexed++
				continue
			}

			fmatch, err := filepath.Match(opts.Filter, strings.ToLower(node.Name))
			if err != nil {
				stats.Errors = append(stats.Errors, err)
				continue
			}

			if !fmatch {
				stats.Mismatch++
				continue
			}
			stats.LastMatch = node.Name

			if doc, ok := opts.DocumentBuilder.ShouldIndex(fileID, *i.IndexEngine, node, repo); ok {
				if opts.AppendFileMeta {
					doc.AddField(bluge.NewTextField("filename", string(node.Name)).StoreValue()).
						AddField(bluge.NewTextField("repository_id", repo.Config().ID).StoreValue()).
						AddField(bluge.NewDateTimeField("mod_time", node.ModTime).StoreValue()).
						AddField(bluge.NewTextField("blobs", marshalBlobIDs(node.Content)).StoreValue()).
						AddField(bluge.NewTextField("parent_tree", blob.String()).StoreValue()).
						AddField(bluge.NewCompositeFieldExcluding("_all", nil))
				}
				err = i.IndexEngine.Index(doc)
				if err != nil {
					stats.Errors = append(stats.Errors, err)
				} else {
					stats.IndexedNodes++
				}
			}
		}
	}

	return stats, i.IndexEngine.Close()
}

func (i Indexer) Search(ctx context.Context, query string, opts SearchOptions) ([]SearchResult, error) {
	maxRes := opts.MaxResults
	if maxRes == 0 {
		maxRes = searchDefaultMaxResults
	}

	idx := i.IndexEngine

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
	count := int64(0)
	match, err := iter.Next()
	for err == nil && match != nil && count < maxRes {
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

func (i Indexer) Close() {
	i.IndexEngine.Close()
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
