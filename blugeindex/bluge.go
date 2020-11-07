package blugeindex

import (
	"context"
	"sync"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/index"
	"github.com/blugelabs/bluge/search"
)

type BlugeIndex struct {
	IndexPath    string
	BatchSize    int
	conf         *bluge.Config
	writer       *bluge.Writer
	writerClosed bool
	docsBatched  int64
	batch        *index.Batch
}

const defaultBatchSize = 1000

var bi *BlugeIndex
var once sync.Once

func Init(indexPath string, batchSize int) *BlugeIndex {
	if bi != nil {
		panic("blugindex already initialized")
	}
	once.Do(func() {
		bi = NewBlugeIndex(indexPath, batchSize)
	})
	return bi
}

func BlugeInstance() *BlugeIndex {
	if bi == nil {
		panic("initialize blugeindex first")
	}
	return bi
}

func NewBlugeIndex(indexPath string, batchSize int) *BlugeIndex {
	blugeConf := bluge.DefaultConfig(indexPath)
	idx := &BlugeIndex{conf: &blugeConf, IndexPath: indexPath, writerClosed: true, BatchSize: batchSize}
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
			err = i.writeBatch()
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

func (i *BlugeIndex) Count() (int64, error) {
	query := bluge.NewWildcardQuery("*").SetField("_id")

	request := bluge.NewAllMatches(query)

	writer, err := i.Writer()
	if err != nil {
		return 0, err
	}

	reader, err := writer.Reader()
	if err != nil {
		return 0, err
	}

	documentMatchIterator, err := reader.Search(context.Background(), request)
	if err != nil {
		return 0, err
	}

	var count int64
	count = 0
	match, err := documentMatchIterator.Next()
	for err == nil && match != nil {
		count++
		match, err = documentMatchIterator.Next()
	}

	return count, err
}

func (i *BlugeIndex) Get(id string) (*search.DocumentMatch, error) {
	var err error

	query := bluge.NewWildcardQuery(id).SetField("_id")
	request := bluge.NewAllMatches(query)

	writer, err := i.Writer()
	if err != nil {
		return nil, err
	}

	reader, err := writer.Reader()
	if err != nil {
		return nil, err
	}

	documentMatchIterator, err := reader.Search(context.Background(), request)
	if err != nil {
		return nil, err
	}

	return documentMatchIterator.Next()
}
