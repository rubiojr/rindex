package blugeindex

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/similarity"
	qs "github.com/blugelabs/query_string"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	lopt "github.com/syndtr/goleveldb/leveldb/opt"
)

var ErrIndexClosed = errors.New("index closed")

type BlugeIndex struct {
	IndexPath   string
	BatchSize   uint
	conf        *index.Config
	docsBatched int64
	batch       *index.Batch
	queue       chan *IndexedDocument
	done        chan bool
	indexed     chan *IndexedDocument
	wg          *sync.WaitGroup
	closed      bool
	once        sync.Once
	writer      *index.Writer
	idBuf       map[string]string
	idCache     *leveldb.DB
	m           sync.Mutex
	indexing    bool
}

type IndexedDocument struct {
	Document *bluge.Document
	Path     string
	Error    error
}

func NewBlugeIndex(indexPath string, batchSize uint) *BlugeIndex {
	idx := &BlugeIndex{conf: defaultConf(indexPath), IndexPath: indexPath, BatchSize: batchSize}
	idx.batch = bluge.NewBatch()
	idx.queue = make(chan *IndexedDocument)
	idx.done = make(chan bool)
	idx.wg = &sync.WaitGroup{}
	idx.indexed = make(chan *IndexedDocument)
	idx.closed = false
	idx.idBuf = map[string]string{}

	return idx
}

func (i *BlugeIndex) SetBatchSize(size uint) {
	i.BatchSize = size
}

func (i *BlugeIndex) Index(doc *bluge.Document, path string) error {
	i.once.Do(func() {
		go func() {
			i.m.Lock()
			i.indexing = true
			i.m.Unlock()

			var err error
			i.idCache, err = i.openIDDB()
			if err != nil {
				panic(err)
			}

			defer func() {
				i.m.Lock()
				i.indexing = false
				i.m.Unlock()
				i.safeIDDBClose()
			}()

			i.writer, err = index.OpenWriter(*i.conf)
			if err != nil {
				panic(err)
			}

			defer i.writer.Close()
			for {
				select {
				case doc := <-i.queue:
					err := i.writeDoc(doc)
					doc.Error = err
					// FIXME: we need proper error handling
					if err != nil {
						panic(err)
					}
					i.indexed <- doc
					i.wg.Done()
				case <-i.done:
					i.closed = true
					return
				default:
				}
			}
		}()
	})

	if i.closed {
		return ErrIndexClosed
	}

	i.wg.Add(1)
	i.queue <- &IndexedDocument{Document: doc, Path: path}

	di := <-i.indexed

	return di.Error
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

	return fn(iter)
}

func (i *BlugeIndex) Close() {
	i.wg.Wait()
	i.writeBatch()
	i.done <- true
	close(i.done)
}

func (i *BlugeIndex) writeBatch() error {
	err := i.writer.Batch(i.batch)
	if err != nil {
		return err
	}

	for fileID, path := range i.idBuf {
		err = i.idCache.Put([]byte(fileID), []byte(path), nil)
		if err != nil {
			panic(err)
		}
	}

	i.idBuf = map[string]string{}
	i.docsBatched = 0
	i.batch.Reset()

	return nil
}

func (i *BlugeIndex) Has(fileID string) (bool, error) {
	if _, ok := i.idBuf[fileID]; ok {
		return true, nil
	}

	var err error
	i.idCache, err = i.openIDDB()
	if err != nil {
		panic(err)
	}
	defer i.safeIDDBClose()

	return i.idCache.Has([]byte(fileID), nil)
}

func (i *BlugeIndex) writeDoc(doc *IndexedDocument) error {
	var err error
	fid := string(doc.Document.ID().Term())

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
	i.m.Lock()
	defer i.m.Unlock()

	if i.idCache != nil {
		return i.idCache, nil
	}

	o := &lopt.Options{
		NoSync:      true,
		Compression: opt.NoCompression,
		// https://github.com/syndtr/goleveldb/issues/212
		OpenFilesCacheCapacity: 100,
		// CompactionTableSizeMultiplier: 2,
	}
	return leveldb.OpenFile(filepath.Join(i.IndexPath, "id.db"), o)
}

func (i *BlugeIndex) safeIDDBClose() {
	i.m.Lock()
	defer i.m.Unlock()

	if !i.indexing {
		i.idCache.Close()
		i.idCache = nil
	}
}
