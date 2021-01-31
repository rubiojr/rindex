package rindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/rs/zerolog"
	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rapi/walker"
	"github.com/rubiojr/rindex/blugeindex"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/vmihailenco/msgpack/v5"
)

var log = zerolog.New(os.Stderr).With().Timestamp().Logger()

// IndexOptions to be passed to Index
type IndexOptions struct {
	// The Filter decides if the file is indexed or not
	Filter Filter
	// Batching improves indexing speed at the cost of using
	// some more memory. Dramatically improves indexing speed
	// for large number of files.
	// 0 or 1 disables batching. Defaults to 1 (no batching) if not set.
	BatchSize uint
	// If set to true, all the repository snapshots and files will be scanned and re-indexed.
	Reindex bool
	// DocumentBuilder is responsible of creating the Bluge document that will be indexed
	DocumentBuilder DocumentBuilder

	filenameAnalyzer *analysis.Analyzer

	Debug bool
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
	cacheMutex  *sync.Mutex
}

// DefaultIndexOptions will:
// * Index all the files found
// * With batching disabled
// * Adding basic file metadata to every file being indexed
// * Using the default document builder
var DefaultIndexOptions = IndexOptions{
	Filter:           &MatchAllFilter{},
	BatchSize:        1,
	DocumentBuilder:  FileDocumentBuilder{},
	filenameAnalyzer: blugeindex.NewFilenameAnalyzer(),
}

func resetEnv() {
	os.Setenv("RESTIC_REPOSITORY", "")
	os.Setenv("RESTIC_PASSWORD", "")
}

func newIndexer(indexPath, repo, pass string) (Indexer, error) {
	if os.Getenv("RINDEX_DEBUG") == "1" {
		log = log.Level(zerolog.DebugLevel)
	} else {
		log = log.Level(zerolog.InfoLevel)
	}

	indexer := &Indexer{}
	if indexPath == "" {
		return *indexer, errors.New("index path can't be empty")
	}

	indexer.IndexPath = indexPath
	indexer.RepositoryLocation = repo
	indexer.RepositoryPassword = pass
	indexer.cacheMutex = &sync.Mutex{}

	return *indexer, nil
}

func NewOffline(indexPath string, repo, pass string) (Indexer, error) {
	indexer, err := newIndexer(indexPath, repo, pass)
	if err != nil {
		return indexer, err
	}

	indexer.IndexEngine, err = blugeindex.OfflineIndex(indexPath)
	if err != nil {
		return indexer, err
	}

	return indexer, err
}

// New creates a new Indexer.
// indexPath is the path to the directory that will contain the index files.
func New(indexPath string, repo, pass string) (Indexer, error) {
	indexer, err := newIndexer(indexPath, repo, pass)
	if err != nil {
		return indexer, err
	}

	indexer.IndexEngine, err = blugeindex.NewBlugeIndex(indexPath, 0)
	if err != nil {
		return indexer, err
	}

	err = indexer.initCaches()

	return indexer, err
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

	if opts.filenameAnalyzer == nil {
		opts.filenameAnalyzer = blugeindex.NewFilenameAnalyzer()
	}

	i.IndexEngine.SetBatchSize(opts.BatchSize)

	stats := NewStats()

	// FIXME: RAPI will override repo path/pass with values from the environment
	// if present. Need to fix this upstream, not a great behaviour for a library.
	resetEnv()
	ropts := rapi.DefaultOptions
	ropts.Password = i.RepositoryPassword
	ropts.Repo = i.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return IndexStats{}, err
	}

	go func() {
		sc, err := listSnapshots(ctx, repo)
		if err != nil {
			stats.ErrorsAdd(err)
			return
		}
		count := uint64(0)
		for _ = range sc {
			count++
		}
		stats.SetTotalSnapshots(count)
	}()

	go func() {
		snaps, err := i.listMissingSnapshots(ctx, repo)
		if err != nil {
			stats.ErrorsAdd(err)
		}
		stats.SetMissingSnapshots(uint64(len(snaps)))
		log.Debug().Msgf("missing snapshots: %d", len(snaps))
	}()

	log.Debug().Msg("loading index")
	if err = repo.LoadIndex(ctx); err != nil {
		return stats, err
	}

	ichan := make(chan blugeindex.Indexable, int(i.IndexEngine.BatchSize))
	indexed := i.IndexEngine.Index(ichan)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for di := range indexed {
			if di.Error != nil {
				stats.ErrorsAdd(err)
			}
		}
		wg.Done()
	}()

	log.Debug().Msg("starting to scan snapshots")
	err = restic.ForAllSnapshots(ctx, repo, nil, func(id restic.ID, snap *restic.Snapshot, err error) error {
		sid := snap.ID().String()
		log.Debug().Msgf("scanning snapshot %s", sid)

		if err != nil {
			stats.ErrorsAdd(err)
			return err
		}

		needsIndexing := true
		log.Debug().Msgf("checking if snapshot %s needs indexing", snap.ID())
		err = i.inCache(func(cache *leveldb.DB) error {
			if _, err := cache.Get(snap.ID()[:], nil); err == nil && !opts.Reindex {
				needsIndexing = false
			}

			return err
		})
		log.Debug().Msgf("snapshot %s needs indexing: %t", snap.ID(), needsIndexing)

		if err != nil {
			stats.ErrorsAdd(err)
		}

		if !needsIndexing {
			return nil
		}

		log.Debug().Msgf("counting files in snapshot %s", sid)
		c, err := countFiles(ctx, repo, snap)
		if err != nil {
			log.Error().Err(err).Msgf("error counting snapshot %s files", sid)
			stats.ErrorsAdd(err)
		}
		log.Debug().Msgf("snapshot %s total files: %d", sid, c)
		stats.SetSnapshotFiles(sid, c)
		stats.SetCurrentSnapshotTotalFiles(c)

		stats.ScannedSnapshotsInc()
		i.walkSnapshot(ctx, repo, snap, &stats, opts, progress, ichan)

		err = i.inCache(func(cache *leveldb.DB) error {
			return cache.Put(snap.ID()[:], []byte{}, nil)
		})
		if err != nil {
			stats.ErrorsAdd(err)
		}

		return err
	})

	if err != nil {
		stats.ErrorsAdd(err)
	}

	close(ichan)
	wg.Wait()

	return stats, nil
}

// FIXME: this can't be called when indexing
func (i *Indexer) MissingSnapshots(ctx context.Context) ([]string, error) {
	missing := []string{}

	// FIXME: RAPI will override repo path/pass with values from the environment
	// if present. Need to fix this upstream, not a great behaviour for a library.
	resetEnv()
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

	snapsFound := uint64(0)
	err = i.inCache(func(cache *leveldb.DB) error {
		for snap := range snaps {
			snapsFound++
			if _, err := cache.Get(snap.ID()[:], nil); err == nil {
				continue
			}
			missing = append(missing, snap.ID().String())
		}

		return nil
	})
	log.Debug().Msgf("snapshots found: %d", snapsFound)

	return missing, err
}

func (i *Indexer) initCaches() error {
	var err error

	indexDir := filepath.Dir(i.IndexPath)
	cacheDir := filepath.Join(indexDir, "cache")

	os.MkdirAll(cacheDir, 0755)

	return err
}

func (i *Indexer) openSnapCache() (*leveldb.DB, error) {
	indexDir := filepath.Dir(i.IndexPath)
	cacheDir := filepath.Join(indexDir, "cache")
	snapCache := filepath.Join(cacheDir, "snap.cache")

	o := &opt.Options{
		NoSync:      true,
		Compression: opt.NoCompression,
		// https://github.com/syndtr/goleveldb/issues/212
		OpenFilesCacheCapacity: 50,
		// CompactionTableSizeMultiplier: 2,
	}

	return leveldb.OpenFile(snapCache, o)
}

func (i *Indexer) scanNode(repo *repository.Repository, repoID string, opts IndexOptions, node *restic.Node, nodepath string, host string, stats *IndexStats, ichan chan blugeindex.Indexable) {
	if node == nil {
		return
	}

	stats.ScannedNodesInc()
	stats.LastMatch = node.Name

	if node.Type != "file" {
		return
	}

	stats.ScannedFilesInc()
	stats.CurrentSnapshotFilesInc()

	if !opts.Filter.ShouldIndex(nodepath) {
		stats.MismatchInc()
		return
	}

	fileID := nodeFileID(node, nodepath)
	bhash := hashBlobs(node)

	present, err := i.IndexEngine.Has(fileID)
	if err != nil {
		stats.ErrorsAdd(err)
		return
	}

	if present {
		stats.AlreadyIndexedInc()
		if !opts.Reindex {
			return
		}
	}

	stats.IndexedFilesInc()

	doc := opts.DocumentBuilder.BuildDocument(fileID, node, repo).
		AddField(bluge.NewStoredOnlyField("blobs", marshalBlobIDs(node.Content, repo.Index()))).
		AddField(bluge.NewTextField("filename", string(node.Name)).WithAnalyzer(opts.filenameAnalyzer).StoreValue()).
		AddField(bluge.NewTextField("repository_id", repoID).StoreValue()).
		AddField(bluge.NewTextField("path", nodepath).StoreValue()).
		AddField(bluge.NewTextField("hostname", host).StoreValue()).
		AddField(bluge.NewTextField("bhash", bhash).StoreValue()).
		AddField(bluge.NewDateTimeField("mtime", node.ModTime).StoreValue()).
		AddField(bluge.NewNumericField("size", float64(node.Size)).StoreValue()).
		AddField(bluge.NewCompositeFieldExcluding("_all", nil))

	log.Debug().Msgf("bhash: %s", bhash)
	ichan <- blugeindex.Indexable{Document: doc, Path: nodepath}
}

func (i *Indexer) Close() {
	i.IndexEngine.Close()
}

func (i *Indexer) walkSnapshot(ctx context.Context, repo *repository.Repository, sn *restic.Snapshot, stats *IndexStats, opts IndexOptions, progress chan IndexStats, ichan chan blugeindex.Indexable) error {
	log.Debug().Msgf("scanning snapshot %s", sn.ID())

	if sn.Tree == nil {
		return fmt.Errorf("snapshot %v has no tree", sn.ID().Str())
	}

	return walker.Walk(ctx, repo, *sn.Tree, nil, func(parentTreeID restic.ID, nodepath string, node *restic.Node, err error) (bool, error) {
		if err != nil {
			log.Error().Err(err).Msgf("Error loading tree %v: %v", parentTreeID, err)
			return false, walker.ErrSkipNode
		}

		i.scanNode(repo, repo.Config().ID, opts, node, nodepath, sn.Hostname, stats, ichan)
		select {
		case progress <- *stats:
		default:
		}
		return true, nil
	})

}

func (i *Indexer) inCache(fn func(cache *leveldb.DB) error) error {
	var err error

	i.cacheMutex.Lock()
	defer i.cacheMutex.Unlock()

	i.snapCache, err = i.openSnapCache()
	if err != nil {
		return err
	}
	defer i.snapCache.Close()
	return fn(i.snapCache)
}

func hashBlobs(node *restic.Node) string {
	var bb []byte
	for _, c := range node.Content {
		bb = append(bb, []byte(c[:])...)
	}
	sha := sha256.Sum256(bb)
	return hex.EncodeToString(sha[:])
}

func nodeFileID(node *restic.Node, path string) string {
	var bb []byte
	for _, c := range node.Content {
		bb = append(bb, []byte(c[:])...)
	}
	bb = append(bb, []byte(path[:])...)
	sha := sha256.Sum256(bb)
	return hex.EncodeToString(sha[:])
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
				log.Error().Err(err).Msgf("could not load snapshot %v: %v\n", id.Str(), err)
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

// needs the index loaded
func countFiles(ctx context.Context, repo *repository.Repository, snap *restic.Snapshot) (uint64, error) {
	fcount := uint64(0)
	sid := snap.ID()

	sn, err := restic.LoadSnapshot(ctx, repo, *sid)
	if err != nil {
		return fcount, err
	}

	err = walker.Walk(ctx, repo, *sn.Tree, nil, func(_ restic.ID, nodepath string, node *restic.Node, err error) (bool, error) {
		if err == nil && node != nil && node.Type == "file" {
			fcount++
		}

		return false, err
	})

	return fcount, err
}
