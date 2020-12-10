package blugeindex

import (
	"context"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
	qs "github.com/blugelabs/query_string"
)

type BlugeIndex struct {
	IndexPath    string
	BatchSize    uint
	conf         *bluge.Config
	writer       *bluge.Writer
	writerClosed bool
	docsBatched  int64
	batch        *index.Batch
	m            *sync.Mutex
}

const defaultBatchSize = 1000

func NewBlugeIndex(indexPath string, batchSize uint) *BlugeIndex {
	blugeConf := bluge.DefaultConfig(indexPath)
	idx := &BlugeIndex{conf: &blugeConf, IndexPath: indexPath, writerClosed: true, BatchSize: batchSize, m: &sync.Mutex{}}
	if batchSize > 1 {
		idx.batch = bluge.NewBatch()
	}
	return idx
}

func (i *BlugeIndex) Writer() (*bluge.Writer, error) {
	var err error
	if i.writerClosed {
		i.writer, err = bluge.OpenWriter(*i.conf)
		if err == nil {
			i.writerClosed = false
		}
	}
	return i.writer, err
}

func (i *BlugeIndex) SetBatchSize(size uint) {
	if size > 1 {
		i.batch = bluge.NewBatch()
	} else {
		i.batch = nil
	}
	i.BatchSize = size
}

func (i *BlugeIndex) Reader() (*bluge.Reader, error) {
	writer, err := i.Writer()
	if err != nil {
		return nil, err
	}
	return writer.Reader()
}

func (i *BlugeIndex) OpenReader() (*bluge.Reader, error) {
	return bluge.OpenReader(*i.conf)
}

func (i *BlugeIndex) IsDirty() bool {
	return i.docsBatched > 0
}

func (i *BlugeIndex) Index(doc *bluge.Document) error {
	var err error
	if i.BatchSize > 1 {
		i.batch.Update(doc.ID(), doc)
		i.docsBatched++
		if i.docsBatched >= int64(i.BatchSize) {
			err = i.Close()
		}
	} else {
		writer, err := i.Writer()
		if err != nil {
			return err
		}
		err = writer.Update(doc.ID(), doc)
	}

	return err
}

func (i *BlugeIndex) Close() error {
	var err error
	if i.BatchSize > 1 {
		err = i.writeBatch()
		if err != nil {
			return err
		}
	}

	if !i.writerClosed {
		err = i.writer.Close()
		i.writerClosed = true
	}
	return err
}

func (i *BlugeIndex) Count() (uint64, error) {
	reader, err := i.Reader()
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	query := bluge.NewMatchAllQuery()
	request := bluge.NewAllMatches(query)

	iter, err := reader.Search(context.Background(), request)
	if err != nil {
		return 0, err
	}

	match, err := iter.Next()
	count := uint64(0)
	for err == nil && match != nil {
		count++
		match, err = iter.Next()
	}

	return count, nil
}

func (i *BlugeIndex) Search(q string, field string) (search.DocumentMatchIterator, error) {
	reader, err := i.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return i.SearchWithReader(q, field, reader)
}

// Warning: search queries with a large number of arguments can eat all your memory
// when using globbing
func (i *BlugeIndex) SearchWithQuery(q string) (search.DocumentMatchIterator, error) {
	reader, err := i.Reader()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return i.SearchWithReaderAndQuery(q, reader)
}

func (i *BlugeIndex) SearchWithReaderAndQuery(q string, reader *bluge.Reader) (search.DocumentMatchIterator, error) {
	if q == "*" {
		q = "_id:*"
	}

	query, err := qs.ParseQueryString(q, qs.DefaultOptions())
	if err != nil {
		return nil, err
	}

	request := bluge.NewAllMatches(query)

	return reader.Search(context.Background(), request)
}

func (i *BlugeIndex) SearchWithReader(q string, field string, reader *bluge.Reader) (search.DocumentMatchIterator, error) {
	query := bluge.NewMatchQuery(q)
	if field != "" {
		query = query.SetField(field)
	}
	request := bluge.NewTopNSearch(100, query)
	return reader.Search(context.Background(), request)
}

func (i *BlugeIndex) writeBatch() error {
	if !i.IsDirty() {
		return nil
	}

	writer, err := i.Writer()
	if err != nil {
		return err
	}

	err = writer.Batch(i.batch)
	if err != nil {
		return err
	}
	i.batch.Reset()
	i.docsBatched = 0
	return nil
}
