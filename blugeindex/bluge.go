package blugeindex

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/similarity"
	"github.com/rubiojr/rindex/internal/qs"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	lopt "github.com/syndtr/goleveldb/leveldb/opt"
)

type BlugeIndex struct {
	IndexPath   string
	BatchSize   uint
	conf        *index.Config
	docsBatched int64
	batch       *index.Batch
	writer      *index.Writer
	idBuf       map[string]string
	idCache     *leveldb.DB
	m           sync.Mutex
	indexing    bool
	offline     bool
}

type IndexedDocument struct {
	Document *bluge.Document
	Path     string
	Error    error
}

var ErrInvalidIndexPath = errors.New("invalid index path")
var ErrLevelDBOpen = errors.New("failed to open the ID database")
var ErrOfflineIndex = errors.New("offline indexer, operation not supported")

// Can be used from a different process
func OfflineIndex(indexPath string, batchSize uint) (*BlugeIndex, error) {
	if indexPath == "" {
		panic(fmt.Errorf("%w: path '%s'", ErrInvalidIndexPath, indexPath))
	}

	idx := &BlugeIndex{conf: defaultConf(indexPath), IndexPath: indexPath, BatchSize: batchSize}
	idx.offline = true

	return idx, nil
}

func NewBlugeIndex(indexPath string, batchSize uint) (*BlugeIndex, error) {
	if indexPath == "" {
		panic(fmt.Errorf("%w: path '%s'", ErrInvalidIndexPath, indexPath))
	}

	idx := &BlugeIndex{conf: defaultConf(indexPath), IndexPath: indexPath, BatchSize: batchSize}
	idx.batch = bluge.NewBatch()
	idx.idBuf = map[string]string{}
	var err error
	idx.idCache, err = idx.openIDDB()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLevelDBOpen, err)
	}

	return idx, nil
}

func (i *BlugeIndex) SetBatchSize(size uint) {
	i.BatchSize = size
}

type Indexable struct {
	Document *bluge.Document
	Path     string
}

type DocumentIndexed struct {
	Document *bluge.Document
	Error    error
}

func (i *BlugeIndex) Index(docs chan Indexable) chan DocumentIndexed {
	if i.offline {
		panic(ErrOfflineIndex)
	}

	ch := make(chan DocumentIndexed)

	go func() {
		if i.indexing {
			ch <- DocumentIndexed{Document: nil, Error: errors.New("indexing in progress")}
		}

		var err error
		i.writer, err = index.OpenWriter(*i.conf)
		if err != nil {
			ch <- DocumentIndexed{Document: nil, Error: err}
		}

		for doc := range docs {
			err := i.writeDoc(&doc)
			ch <- DocumentIndexed{Document: doc.Document, Error: err}
		}
		i.writeBatch()
		i.writer.Close()
		close(ch)
	}()

	return ch
}

func (i *BlugeIndex) Count() (uint64, error) {
	count := uint64(0)
	err := i.Search("*", func(iter search.DocumentMatchIterator) error {
		match, err := iter.Next()
		if err != nil {
			panic(err)
		}
		for err == nil && match != nil {
			count++
			match, err = iter.Next()
		}

		return nil
	})
	return count, err
}

func (i *BlugeIndex) Search(q string, fn func(search.DocumentMatchIterator) error) error {
	if q == "*" {
		q = "_id:*"
	}

	query, err := qs.ParseQueryString(q, qs.DefaultOptions())
	if err != nil {
		return err
	}

	request := bluge.NewAllMatches(query)

	reader, err := i.openOfflineReader()
	if err != nil {
		return err
	}
	defer reader.Close()

	iter, err := reader.Search(context.Background(), request)
	if err != nil {
		return err
	}

	return fn(iter)
}

func (i *BlugeIndex) Close() {
	if i.offline {
		panic(ErrOfflineIndex)
	}

	i.idCache.Close()
}

func (i *BlugeIndex) writeBatch() error {
	err := i.writer.Batch(i.batch)
	if err != nil {
		return err
	}
	i.docsBatched = 0
	i.batch.Reset()

	for fileID, path := range i.idBuf {
		err = i.idCache.Put([]byte(fileID), []byte(path), nil)
		if err != nil {
			panic(err)
		}
	}

	i.idBuf = map[string]string{}

	return nil
}

func (i *BlugeIndex) Has(fileID string) (bool, error) {
	if i.offline {
		panic(ErrOfflineIndex)
	}

	i.m.Lock()
	defer i.m.Unlock()

	if _, ok := i.idBuf[fileID]; ok {
		return true, nil
	}

	return i.idCache.Has([]byte(fileID), nil)
}

func (i *BlugeIndex) writeDoc(doc *Indexable) error {
	var err error
	fid := string(doc.Document.ID().Term())

	i.m.Lock()
	defer i.m.Unlock()
	i.idBuf[fid] = doc.Path
	i.batch.Update(doc.Document.ID(), doc.Document)
	i.docsBatched++
	if i.docsBatched >= int64(i.BatchSize) {
		err = i.writeBatch()
	}

	return err
}

func (i *BlugeIndex) openOfflineReader() (*bluge.Reader, error) {
	return bluge.OpenReader(bluge.DefaultConfig(i.IndexPath))
}

func defaultConf(path string) *index.Config {
	indexConfig := index.DefaultConfig(path)
	allDocsFields := bluge.NewKeywordField("", "")
	_ = allDocsFields.Analyze(0)
	indexConfig = indexConfig.WithVirtualField(allDocsFields)
	indexConfig = indexConfig.WithNormCalc(func(field string, length int) float32 {
		return similarity.NewBM25Similarity().ComputeNorm(length)
	})

	// Causes trouble for searches in tests currently
	// Need to try persisted callback, as described here https://github.com/blevesearch/bleve/issues/1266
	//indexConfig = indexConfig.WithUnsafeBatches()

	// helps with file descriptor and memory usage
	indexConfig = indexConfig.WithPersisterNapTimeMSec(50)

	// Also from https://github.com/blevesearch/bleve/issues/1266
	indexConfig.PersisterNapUnderNumFiles = 300

	return &indexConfig
}

func (i *BlugeIndex) openIDDB() (*leveldb.DB, error) {
	o := &lopt.Options{
		NoSync:      true,
		Compression: opt.NoCompression,
		// https://github.com/syndtr/goleveldb/issues/212
		OpenFilesCacheCapacity: 100,
		// CompactionTableSizeMultiplier: 2,
	}
	return leveldb.OpenFile(filepath.Join(i.IndexPath, "id.db"), o)
}
