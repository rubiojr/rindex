package rindex

import (
	"context"
	"errors"

	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
)

var batch = bluge.NewBatch()
var docsWritten = 0

const defaultBatchSize = 10000

type FileIndexConfig struct {
	DocumentBuilder DocumentBuilder
	BatchSize       int
	Repository      *repository.Repository
	IndexPath       string
}

type FileIndex struct {
	config *FileIndexConfig
}

type FileDocumentBuilder struct {
}

func NewFileIndexer(config *FileIndexConfig) (*FileIndex, error) {
	if config.BatchSize == 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.Repository == nil {
		return nil, errors.New("invalid repository object")
	}
	if config.DocumentBuilder == nil {
		config.DocumentBuilder = &FileDocumentBuilder{}
	}

	if config.IndexPath == "" {
		return nil, errors.New("invalid index path")
	}

	f := &FileIndex{config: config}
	return f, nil
}

func (b *FileDocumentBuilder) BuildDocument(node *restic.Node, repository *repository.Repository) (*bluge.Document, error) {
	fileID := NodeFileID(node)
	//TODO: optionally save node metadata
	//nodeJSON, err := node.MarshalJSON()
	//if err != nil {
	//	results <- err
	//	return
	//}
	doc := bluge.NewDocument(fileID.String()).
		AddField(bluge.NewTextField("filename", string(node.Name)).StoreValue().HighlightMatches()).
		AddField(bluge.NewTextField("blobs", MarshalBlobIDs(node.Content)).StoreValue()).
		//AddField(bluge.NewTextField("metadata", string(nodeJSON)).StoreValue().HighlightMatches()).
		AddField(bluge.NewDateTimeField("mod_time", node.ModTime).StoreValue().HighlightMatches()).
		AddField(bluge.NewTextField("repository_location", repository.Backend().Location()).StoreValue().HighlightMatches()).
		AddField(bluge.NewTextField("repository_id", repository.Config().ID).StoreValue().HighlightMatches())

	return doc, nil
}

func (i *FileIndex) WasIndexed(id *FileID) (bool, error) {
	var err error

	query := bluge.NewWildcardQuery(id.String()).SetField("_id")
	request := bluge.NewAllMatches(query)

	documentMatchIterator, err := blugeReader(i.config.IndexPath).Search(context.Background(), request)
	if err != nil {
		panic(err)
	}

	match, err := documentMatchIterator.Next()
	if err == nil && match != nil {
		return true, nil
	}

	return false, nil
}

//TODO: not thread safe
func (i *FileIndex) Index(node *restic.Node, results chan error) {
	if node == nil {
		results <- NilNodeFoundError
		return
	}

	fileID := NodeFileID(node)
	if ok, _ := i.WasIndexed(fileID); ok {
		results <- AlreadyIndexedError
		return
	}

	doc, err := i.config.DocumentBuilder.BuildDocument(node, i.config.Repository)
	if err != nil {
		results <- err
		return
	}

	if i.config.BatchSize > 1 {
		batch.Update(doc.ID(), doc)
		docsWritten++
		if docsWritten >= i.config.BatchSize {
			err = blugeWriter(i.config.IndexPath).Batch(batch)
			batch = bluge.NewBatch()
			docsWritten = 0
		}
	} else {
		err = blugeWriter(i.config.IndexPath).Update(doc.ID(), doc)
	}
	results <- err
}

func (i *FileIndex) Close() error {
	return blugeWriter(i.config.IndexPath).Close()
}
