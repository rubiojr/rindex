package blugeindex

import (
	"context"
	"errors"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
	qs "github.com/blugelabs/query_string"
)

var ErrIndexClosed = errors.New("index closed")

type BlugeIndex struct {
	IndexPath   string
	BatchSize   uint
	conf        *bluge.Config
	docsBatched int64
	batch       *index.Batch
	queue       chan *bluge.Document
	done        chan bool
	indexed     chan *IndexedDocument
	wg          *sync.WaitGroup
	closed      bool
	m           sync.Mutex
	once        sync.Once
	writer      *bluge.Writer
}

type IndexedDocument struct {
	Document *bluge.Document
	Error    error
}

func NewBlugeIndex(indexPath string, batchSize uint) *BlugeIndex {
	blugeConf := bluge.DefaultConfig(indexPath)
	idx := &BlugeIndex{conf: &blugeConf, IndexPath: indexPath, BatchSize: batchSize}
	idx.batch = bluge.NewBatch()
	idx.queue = make(chan *bluge.Document)
	idx.done = make(chan bool)
	idx.wg = &sync.WaitGroup{}
	idx.indexed = make(chan *IndexedDocument)
	idx.closed = false
	idx.m = sync.Mutex{}

	return idx
}

func (i *BlugeIndex) SetBatchSize(size uint) {
	i.m.Lock()
	defer i.m.Unlock()

	i.BatchSize = size
}

func (i *BlugeIndex) Index(doc *bluge.Document) error {
	i.once.Do(func() {
		go func() {
			var err error
			i.writer, err = bluge.OpenWriter(*i.conf)
			if err != nil {
				panic(err)
			}
			defer i.writer.Close()
			for {
				select {
				case doc := <-i.queue:
					err := i.writeDoc(doc)
					i.indexed <- &IndexedDocument{Document: doc, Error: err}
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
	i.queue <- doc

	for doc := range i.indexed {
		return doc.Error
	}

	return nil
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
	i.m.Lock()
	defer i.m.Unlock()

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

	i.docsBatched = 0
	i.batch.Reset()

	return nil
}

func (i *BlugeIndex) writeDoc(doc *bluge.Document) error {
	var err error
	i.batch.Update(doc.ID(), doc)
	i.docsBatched++
	if i.docsBatched >= int64(i.BatchSize) {
		err = i.writeBatch()
	}

	return err
}

func (i *BlugeIndex) openOfflineReader() (*bluge.Reader, error) {
	return bluge.OpenReader(*i.conf)
}
