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
	reader      *bluge.Reader
	writer      *bluge.Writer
	m           sync.Mutex
}

type IndexedDocument struct {
	Document *bluge.Document
	Error    error
}

func NewBlugeIndex(indexPath string, batchSize uint) *BlugeIndex {
	var err error
	blugeConf := bluge.DefaultConfig(indexPath)
	idx := &BlugeIndex{conf: &blugeConf, IndexPath: indexPath, BatchSize: batchSize}
	idx.batch = bluge.NewBatch()
	idx.queue = make(chan *bluge.Document)
	idx.done = make(chan bool)
	idx.wg = &sync.WaitGroup{}
	idx.indexed = make(chan *IndexedDocument)
	idx.closed = false
	idx.m = sync.Mutex{}

	idx.writer, err = bluge.OpenWriter(blugeConf)
	if err != nil {
		panic(err)
	}
	idx.reader, err = idx.writer.Reader()
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case doc := <-idx.queue:
				err := idx.writeDoc(doc)
				idx.indexed <- &IndexedDocument{Document: doc, Error: err}
				idx.wg.Done()
			case <-idx.done:
				idx.closed = true
				return
			}
		}
	}()

	return idx
}

func (i *BlugeIndex) SetBatchSize(size uint) {
	i.m.Lock()
	defer i.m.Unlock()

	i.BatchSize = size
}

func (i *BlugeIndex) Index(doc *bluge.Document) error {
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
	iter, err := i.Search("*")
	if err != nil {
		return 0, err
	}

	match, err := iter.Next()
	if err != nil {
		panic(err)
	}
	count := uint64(0)
	for err == nil && match != nil {
		count++
		match, err = iter.Next()
	}

	return count, nil
}

func (i *BlugeIndex) Search(q string) (search.DocumentMatchIterator, error) {
	if q == "*" {
		q = "_id:*"
	}

	query, err := qs.ParseQueryString(q, qs.DefaultOptions())
	if err != nil {
		return nil, err
	}

	request := bluge.NewAllMatches(query)

	reader, err := i.openReader()
	if err != nil {
		return nil, err
	}

	return reader.Search(context.Background(), request)
}

func (idx *BlugeIndex) Close() {
	idx.m.Lock()
	defer idx.m.Unlock()

	idx.flush()
	idx.done <- true
	idx.reader.Close()
	idx.writer.Close()
	close(idx.done)
}

func (i *BlugeIndex) writeBatch() error {
	err := i.writer.Batch(i.batch)
	if err != nil {
		return err
	}
	i.batch.Reset()
	i.docsBatched = 0

	return nil
}

func (idx *BlugeIndex) flush() {
	idx.wg.Wait()
	err := idx.writeBatch()
	if err != nil {
		panic(err)
	}
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

func (i *BlugeIndex) openReader() (*bluge.Reader, error) {
	i.m.Lock()
	defer i.m.Unlock()

	if i.closed {
		return nil, errors.New("index closed")
	}

	var err error
	i.reader.Close()
	i.reader, err = i.writer.Reader()

	return i.reader, err
}
