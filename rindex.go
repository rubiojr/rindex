package rindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rapi/walker"
	"github.com/rubiojr/rindex/blugeindex"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	lopt "github.com/syndtr/goleveldb/leveldb/opt"
)

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
}

// IndexOptions to be passed to Index
type IndexOptions struct {
	// The Filter decides if the file is indexed or not
	Filter Filter
	// Batching improves indexing speed at the cost of using
	// some more memory. Dramatically improves indexing speed
	// for large number of files.
	// 0 or 1 disables batching. Defaults to 1 (no batching) if not set.
	BatchSize uint
	// If set to true, basic file metadata will be added to every document
	// indexed:
	//
	//   repository_id: Restic's repository ID
	//   path: the path of the file when it was backed up
	//   hostname: the host that was backed up
	//   mtime: file modification time when it was backed up
	//   blobs: raw data blobs that form the file
	//   size: file size when it was backed up
	//
	// If set to false, the DocumentBuilder implementing BuildDocument is fully responsible
	// for the information to be indexed
	AppendFileMeta bool
	// If set to true, all the repository snapshots and files will be scanned and re-indexed.
	Reindex bool
	// DocumentBuilder is responsible of creating the Bluge document that will be indexed
	DocumentBuilder DocumentBuilder
}

type Indexer struct {
	// The Restic repository location (RESTIC_REPOSITORY)
	RepositoryLocation string
	// The Restic repository password (RESTIC_PASSWORD)
	RepositoryPassword string
	// The path to the directory that will hold the index
	IndexPath string
	// IndexingEngine being used (Bluge is the only one supported right now)
	IndexEngine *blugeindex.BlugeIndex
	idCache     *leveldb.DB
	idTmpCache  *leveldb.DB
	snapCache   *leveldb.DB
}

// DefaultIndexOptions will:
// * Index all the files found
// * With batching disabled
// * Adding basic file metadata to every file being indexed
// * Using the default document builder
var DefaultIndexOptions = IndexOptions{
	Filter:          &MatchAllFilter{},
	BatchSize:       1,
	AppendFileMeta:  true,
	DocumentBuilder: FileDocumentBuilder{},
}

// New creates a new Indexer.
// indexPath is the path to the directory that will contain the index files.
func New(indexPath string, repo, pass string) (Indexer, error) {
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
	indexer.RepositoryLocation = repo
	indexer.RepositoryPassword = pass

	return indexer, nil
}

// Index will start indexing the repository.
//
// A channel can be passed to follow the indexing process in real time. IndexStats is sent to
// the channel every time a new file is indexed.
func (i *Indexer) Index(ctx context.Context, opts IndexOptions, progress chan IndexStats) (IndexStats, error) {
	var err error

	if opts.DocumentBuilder == nil {
		opts.DocumentBuilder = FileDocumentBuilder{}
	}
	if opts.Filter == nil {
		opts.Filter = &MatchAllFilter{}
	}

	i.IndexEngine.SetBatchSize(opts.BatchSize)
	stats := IndexStats{Errors: []error{}}

	err = i.initCaches()
	if err != nil {
		return stats, err
	}

	ropts := rapi.DefaultOptions
	ropts.Password = i.RepositoryPassword
	ropts.Repo = i.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return IndexStats{}, err
	}

	if err = repo.LoadIndex(ctx); err != nil {
		return stats, err
	}

	snaps, _ := listSnapshots(ctx, repo)
	for snap := range snaps {
		if _, err := i.snapCache.Get(snap.ID()[:], nil); err == nil && !opts.Reindex {
			continue
		}
		stats.ScannedSnapshots++
		i.walkSnapshot(ctx, repo, snap, &stats, opts, progress)
		err := i.snapCache.Put(snap.ID()[:], []byte{}, nil)
		if err != nil {
			stats.Errors = append(stats.Errors, err)
		}
	}
	i.Close()
	return stats, nil
}

func (i *Indexer) initCaches() error {
	var err error

	indexDir := filepath.Dir(i.IndexPath)
	cacheDir := filepath.Join(indexDir, "cache")
	idCache := filepath.Join(cacheDir, "id.cache")
	idTmpCache := filepath.Join(cacheDir, "idtmp.cache")
	snapCache := filepath.Join(cacheDir, "snap.cache")

	os.MkdirAll(cacheDir, 0755)

	o := &lopt.Options{
		Filter: filter.NewBloomFilter(10),
		NoSync: true,
	}

	i.idCache, err = leveldb.OpenFile(idCache, o)
	if err != nil {
		return err
	}

	i.snapCache, err = leveldb.OpenFile(snapCache, o)
	if err != nil {
		return err
	}

	err = os.RemoveAll(idTmpCache)
	if err != nil {
		return err
	}

	i.idTmpCache, err = leveldb.OpenFile(idTmpCache, o)
	return err
}

func (i *Indexer) needsIndexing(fileID []byte, reindex bool) bool {
	var err error
	// we've visited this node already
	// This prevents re-indexing duplicated files
	if _, err = i.idTmpCache.Get(fileID, nil); err == nil {
		return false
	}

	// node not visited this run but maybe was indexed previously
	if _, err = i.idCache.Get(fileID, nil); err == nil && !reindex {
		return false
	}

	return true
}

func (i *Indexer) scanNode(repo *repository.Repository, repoID string, opts IndexOptions, node *restic.Node, nodepath string, host string, stats *IndexStats) {
	if node == nil {
		return
	}

	stats.ScannedNodes++

	if node.Type != "file" {
		return
	}

	stats.ScannedFiles++

	if !opts.Filter.ShouldIndex(nodepath) {
		stats.Mismatch++
		return
	}

	fileIDBytes := nodeFileID(node)

	if !i.needsIndexing(fileIDBytes, opts.Reindex) {
		stats.AlreadyIndexed++
		return
	}

	fileID := hex.EncodeToString(fileIDBytes)

	stats.LastMatch = node.Name

	doc := opts.DocumentBuilder.BuildDocument(fileID, node, repo)
	doc.AddField(bluge.NewTextField("blobs", marshalBlobIDs(node.Content, repo.Index())).StoreValue())
	if opts.AppendFileMeta {
		doc.AddField(bluge.NewTextField("filename", string(node.Name)).StoreValue()).
			AddField(bluge.NewTextField("repository_id", repoID).StoreValue()).
			AddField(bluge.NewTextField("path", nodepath).StoreValue()).
			AddField(bluge.NewTextField("hostname", host).StoreValue()).
			AddField(bluge.NewDateTimeField("mtime", node.ModTime).StoreValue()).
			AddField(bluge.NewNumericField("size", float64(node.Size)).StoreValue()).
			AddField(bluge.NewCompositeFieldExcluding("_all", nil))
	}
	err := i.IndexEngine.Index(doc)
	if err != nil {
		stats.Errors = append(stats.Errors, err)
	} else {
		stats.IndexedFiles++
		err = i.addToCaches(fileIDBytes)
		if err != nil {
			stats.Errors = append(stats.Errors, err)
		}
	}
}

func (i *Indexer) addToCaches(fileID []byte) error {
	err := i.idCache.Put(fileID, []byte{}, nil)
	if err != nil {
		return err
	}
	return i.idTmpCache.Put(fileID, []byte{}, nil)
}

func (i *Indexer) Close() {
	err := i.IndexEngine.Close()
	if err != nil {
		panic(err)
	}
	err = i.idCache.Close()
	if err != nil {
		panic(err)
	}
	err = i.idTmpCache.Close()
	if err != nil {
		panic(err)
	}
	err = i.snapCache.Close()
	if err != nil {
		panic(err)
	}
}

func (i *Indexer) walkSnapshot(ctx context.Context, repo *repository.Repository, sn *restic.Snapshot, stats *IndexStats, opts IndexOptions, progress chan IndexStats) error {
	if sn.Tree == nil {
		return fmt.Errorf("snapshot %v has no tree", sn.ID().Str())
	}

	return walker.Walk(ctx, repo, *sn.Tree, nil, func(parentTreeID restic.ID, nodepath string, node *restic.Node, err error) (bool, error) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading tree %v: %v\n", parentTreeID, err)

			return false, walker.ErrSkipNode
		}

		i.scanNode(repo, repo.Config().ID, opts, node, nodepath, sn.Hostname, stats)
		select {
		case progress <- *stats:
		default:
		}
		return true, nil
	})

}

func nodeFileID(node *restic.Node) []byte {
	var bb []byte
	for _, c := range node.Content {
		bb = append(bb, []byte(c[:])...)
	}
	sha := sha256.Sum256(bb)
	return sha[:]
}

func marshalBlobIDs(ids restic.IDs, idx restic.MasterIndex) string {
	pblist := []restic.PackedBlob{}
	for _, id := range ids {
		pb := idx.Lookup(restic.BlobHandle{ID: id, Type: restic.DataBlob})
		pblist = append(pblist, pb...)
	}
	j, err := json.Marshal(pblist)
	if err != nil {
		panic(err)
	}
	return string(j)
}

func listSnapshots(ctx context.Context, repo *repository.Repository) (<-chan *restic.Snapshot, <-chan error) {
	out := make(chan *restic.Snapshot)
	errc := make(chan error, 1)
	go func() {
		defer close(out)

		snapshots := []*restic.Snapshot{}

		err := repo.List(ctx, restic.SnapshotFile, func(id restic.ID, size int64) error {
			sn, err := restic.LoadSnapshot(ctx, repo, id)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not load snapshot %v: %v\n", id.Str(), err)
				return nil
			}
			snapshots = append(snapshots, sn)
			return nil
		})

		if err != nil {
			errc <- err
			return
		}

		for _, sn := range snapshots {
			select {
			case <-ctx.Done():
				return
			case out <- sn:
			}
		}
	}()

	return out, errc
}
