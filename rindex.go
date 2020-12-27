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
	"github.com/blugelabs/bluge/analysis"
	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rapi/walker"
	"github.com/rubiojr/rindex/blugeindex"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	lopt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/vmihailenco/msgpack/v5"
)

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

	filenameAnalyzer *analysis.Analyzer
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
	snapCache   *leveldb.DB
}

// DefaultIndexOptions will:
// * Index all the files found
// * With batching disabled
// * Adding basic file metadata to every file being indexed
// * Using the default document builder
var DefaultIndexOptions = IndexOptions{
	Filter:           &MatchAllFilter{},
	BatchSize:        1,
	AppendFileMeta:   true,
	DocumentBuilder:  FileDocumentBuilder{},
	filenameAnalyzer: blugeindex.NewFilenameAnalyzer(),
}

// New creates a new Indexer.
// indexPath is the path to the directory that will contain the index files.
func New(indexPath string, repo, pass string) (Indexer, error) {
	indexer := Indexer{}
	if indexPath == "" {
		return indexer, errors.New("index path can't be empty")
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
	stats := NewStats()

	err = i.initCaches()
	if err != nil {
		return stats, err
	}
	defer i.close()

	ropts := rapi.DefaultOptions
	ropts.Password = i.RepositoryPassword
	ropts.Repo = i.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return IndexStats{}, err
	}

	go func() {
		snaps, err := i.listMissingSnapshots(ctx, repo)
		if err != nil {
			stats.ErrorsAdd(err)
		}
		stats.SetMissingSnapshots(uint64(len(snaps)))
	}()

	if err = repo.LoadIndex(ctx); err != nil {
		return stats, err
	}

	err = restic.ForAllSnapshots(ctx, repo, nil, func(id restic.ID, snap *restic.Snapshot, err error) error {
		if err != nil {
			stats.ErrorsAdd(err)
			return err
		}

		if _, err := i.snapCache.Get(snap.ID()[:], nil); err == nil && !opts.Reindex {
			return nil
		}

		stats.ScannedSnapshotsInc()
		i.walkSnapshot(ctx, repo, snap, &stats, opts, progress)
		err = i.snapCache.Put(snap.ID()[:], []byte{}, nil)
		if err != nil {
			stats.ErrorsAdd(err)
		}

		return err
	})

	if err != nil {
		stats.ErrorsAdd(err)
	}

	return stats, nil
}

// FIXME: this can't be called when indexing
func (i *Indexer) MissingSnapshots(ctx context.Context) ([]string, error) {
	missing := []string{}

	err := i.initCaches()
	if err != nil {
		return missing, err
	}
	defer i.snapCache.Close()

	ropts := rapi.DefaultOptions
	ropts.Password = i.RepositoryPassword
	ropts.Repo = i.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return missing, err
	}

	return i.listMissingSnapshots(ctx, repo)
}

func (i Indexer) listMissingSnapshots(ctx context.Context, repo *repository.Repository) ([]string, error) {
	var missing []string
	snaps, err := listSnapshots(ctx, repo)
	if err != nil {
		return missing, err
	}
	for snap := range snaps {
		if _, err := i.snapCache.Get(snap.ID()[:], nil); err == nil {
			continue
		}
		missing = append(missing, snap.ID().String())
	}

	return missing, err
}

func (i *Indexer) initCaches() error {
	var err error

	indexDir := filepath.Dir(i.IndexPath)
	cacheDir := filepath.Join(indexDir, "cache")
	snapCache := filepath.Join(cacheDir, "snap.cache")

	os.MkdirAll(cacheDir, 0755)

	o := &lopt.Options{
		NoSync:      true,
		Compression: opt.NoCompression,
		// https://github.com/syndtr/goleveldb/issues/212
		OpenFilesCacheCapacity: 50,
		// CompactionTableSizeMultiplier: 2,
	}

	i.snapCache, err = leveldb.OpenFile(snapCache, o)

	return err
}

func (i *Indexer) scanNode(repo *repository.Repository, repoID string, opts IndexOptions, node *restic.Node, nodepath string, host string, stats *IndexStats) {
	if node == nil {
		return
	}

	stats.ScannedNodesInc()

	if node.Type != "file" {
		return
	}

	stats.ScannedFilesInc()

	if !opts.Filter.ShouldIndex(nodepath) {
		stats.MismatchInc()
		return
	}

	fileID := hex.EncodeToString(nodeFileID(node))

	altPaths, err := i.IndexEngine.BatchedPathsFor(fileID)
	if err != nil {
		panic(err)
	}

	for _, p := range altPaths {
		if p == nodepath && !opts.Reindex {
			stats.AlreadyIndexedInc()
			return
		}
	}
	altPaths = append(altPaths, nodepath)

	if ok, err := i.IndexEngine.Has(fileID); !ok {
		if err != nil {
			panic(err)
		}
		stats.IndexedFilesInc()
	}
	stats.LastMatch = node.Name

	doc := opts.DocumentBuilder.BuildDocument(fileID, node, repo)
	doc.AddField(bluge.NewStoredOnlyField("blobs", marshalBlobIDs(node.Content, repo.Index())))
	if opts.AppendFileMeta {
		doc.AddField(bluge.NewTextField("filename", string(node.Name)).WithAnalyzer(opts.filenameAnalyzer).StoreValue()).
			AddField(bluge.NewTextField("repository_id", repoID).StoreValue()).
			AddField(bluge.NewTextField("path", nodepath).StoreValue()).
			AddField(bluge.NewTextField("hostname", host).StoreValue()).
			AddField(bluge.NewDateTimeField("mtime", node.ModTime).StoreValue()).
			AddField(bluge.NewNumericField("size", float64(node.Size)).StoreValue()).
			AddField(bluge.NewTextField("alt_paths", marshalAltPaths(altPaths)).StoreValue()).
			AddField(bluge.NewCompositeFieldExcluding("_all", nil))
	}
	err = i.IndexEngine.Index(doc, nodepath)
	if err != nil {
		stats.ErrorsAdd(err)
	}
}

func (i *Indexer) close() {
	err := i.snapCache.Close()
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

func marshalAltPaths(paths []string) string {
	altPaths := map[string]bool{}
	pa := []string{}
	for _, path := range paths {
		if _, ok := altPaths[path]; !ok {
			pa = append(pa, path)
			altPaths[path] = true
		}
	}

	j, err := json.Marshal(pa)
	if err != nil {
		panic(err)
	}

	return string(j)
}

func marshalBlobIDs(ids restic.IDs, idx restic.MasterIndex) []byte {
	pblist := []restic.PackedBlob{}
	for _, id := range ids {
		pb := idx.Lookup(restic.BlobHandle{ID: id, Type: restic.DataBlob})
		pblist = append(pblist, pb...)
	}
	j, err := msgpack.Marshal(pblist)
	if err != nil {
		panic(err)
	}
	return j
}

func listSnapshots(ctx context.Context, repo *repository.Repository) (<-chan *restic.Snapshot, error) {
	out := make(chan *restic.Snapshot)
	var errfound error
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
			errfound = err
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

	return out, errfound
}
