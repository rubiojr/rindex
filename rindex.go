package rindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rindex/blugeindex"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	lopt "github.com/syndtr/goleveldb/leveldb/opt"
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
	MaxResults  int64
	SearchField string
}

type Indexer struct {
	IndexPath   string
	IndexEngine *blugeindex.BlugeIndex
	fileIDCache *leveldb.DB
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
	o := &lopt.Options{
		Filter: filter.NewBloomFilter(10),
		NoSync: true,
	}
	db, err := leveldb.OpenFile(indexPath+".fidcache", o)
	if err != nil {
		panic(err)
	}
	return Indexer{
		IndexEngine: blugeindex.NewBlugeIndex(indexPath, 1),
		IndexPath:   indexPath,
		fileIDCache: db,
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
	repoID := repo.Config().ID

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
			i.scanNode(repo, blob, repoID, opts, node, &stats)
		}
	}

	return stats, i.IndexEngine.Close()
}

func (i Indexer) scanNode(repo *repository.Repository, blob restic.ID, repoID string, opts IndexOptions, node *restic.Node, stats *IndexStats) {
	if node.Type != "file" {
		stats.NonFileNodes++
		return
	}

	fileIDBytes := nodeFileID(node)

	if _, err := i.fileIDCache.Get([]byte(fileIDBytes), nil); err == nil {
		stats.AlreadyIndexed++
		return
	}

	fileID := hex.EncodeToString(fileIDBytes)

	fmatch, err := filepath.Match(opts.Filter, strings.ToLower(node.Name))
	if err != nil {
		stats.Errors = append(stats.Errors, err)
		return
	}

	if !fmatch {
		stats.Mismatch++
		return
	}

	stats.LastMatch = node.Name

	if doc, ok := opts.DocumentBuilder.ShouldIndex(fileID, *i.IndexEngine, node, repo); ok {
		if opts.AppendFileMeta {
			doc.AddField(bluge.NewTextField("filename", string(node.Name)).StoreValue()).
				AddField(bluge.NewTextField("repository_id", repoID).StoreValue()).
				AddField(bluge.NewDateTimeField("mtime", node.ModTime).StoreValue()).
				AddField(bluge.NewTextField("blobs", marshalBlobIDs(node.Content)).StoreValue()).
				AddField(bluge.NewCompositeFieldExcluding("_all", nil))
		}
		err = i.IndexEngine.Index(doc)
		if err != nil {
			stats.Errors = append(stats.Errors, err)
		} else {
			stats.IndexedNodes++
			err := i.fileIDCache.Put(fileIDBytes, []byte{}, nil)
			if err != nil {
				stats.Errors = append(stats.Errors, err)
			}
		}
	}
}

func (i Indexer) Search(ctx context.Context, query string, results chan SearchResult, opts SearchOptions) (uint64, error) {
	maxRes := opts.MaxResults
	if maxRes == 0 {
		maxRes = searchDefaultMaxResults
	}

	idx := i.IndexEngine

	reader, err := idx.OpenReader()
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	iter, err := idx.SearchWithReaderAndQuery(query, reader)
	if err != nil {
		return 0, err
	}

	var count uint64
	match, err := iter.Next()
	for err == nil && match != nil {
		searchResult := &SearchResult{}
		err = match.VisitStoredFields(func(field string, value []byte) bool {
			(*searchResult)[field] = value
			return true
		})
		if err == nil {
			count++
			select {
			case results <- *searchResult:
			default:
			}
		}
		match, err = iter.Next()
	}

	return count, err
}

func (i Indexer) Close() {
	i.IndexEngine.Close()
	i.fileIDCache.Close()
}

func nodeFileID(node *restic.Node) []byte {
	var bb []byte
	for _, c := range node.Content {
		bb = append(bb, []byte(c[:])...)
	}
	sha := sha256.Sum256(bb)
	return sha[:]
}

func marshalBlobIDs(ids restic.IDs) string {
	j, err := json.Marshal(ids)
	if err != nil {
		panic(err)
	}
	return string(j)
}
