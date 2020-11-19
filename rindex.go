package rindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
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

type Indexer struct {
	IndexPath   string
	IndexEngine *blugeindex.BlugeIndex
	fileIDCache *leveldb.DB
}

var DefaultIndexOptions = IndexOptions{
	Filter:          "*",
	BatchSize:       1,
	AppendFileMeta:  true,
	DocumentBuilder: FileDocumentBuilder{},
}

func New(indexPath string) (Indexer, error) {
	indexer := Indexer{}
	if indexPath == "" {
		return indexer, errors.New("index path can't be empty")
	}

	err := os.MkdirAll(indexPath, 0755)
	if err != nil {
		return indexer, err
	}

	indexer.IndexEngine = blugeindex.NewBlugeIndex(indexPath, 1)
	indexer.IndexPath = indexPath

	return indexer, nil
}

func (i Indexer) Index(ctx context.Context, opts IndexOptions, progress chan IndexStats) (IndexStats, error) {
	var err error

	if opts.DocumentBuilder == nil {
		opts.DocumentBuilder = FileDocumentBuilder{}
	}
	if opts.Filter == "" {
		opts.Filter = "*"
	}

	i.IndexEngine.SetBatchSize(opts.BatchSize)
	stats := IndexStats{Errors: []error{}}

	o := &lopt.Options{
		Filter: filter.NewBloomFilter(10),
		NoSync: true,
	}
	i.fileIDCache, err = leveldb.OpenFile(i.IndexPath+".fidcache", o)
	if err != nil {
		return stats, nil
	}

	ropts := rapi.DefaultOptions
	ropts.Password = opts.RepositoryPassword
	ropts.Repo = opts.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return IndexStats{}, err
	}
	repoID := repo.Config().ID

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

	i.Close()
	return stats, nil
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
				AddField(bluge.NewNumericField("size", float64(node.Size)).StoreValue()).
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

func (i Indexer) Close() {
	err := i.IndexEngine.Close()
	if err != nil {
		panic(err)
	}
	err = i.fileIDCache.Close()
	if err != nil {
		panic(err)
	}
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
