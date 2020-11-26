package rindex

import (
	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
)

// DocumentBuilder is the interface custom indexers should implement.
type DocumentBuilder interface {
	// BuildDocument returns a new Bluge document to be indexed.
	// The Restic node (file or directory) to be index is passed as an argument so third
	// party implementation can decide the node information they want indexed.
	BuildDocument(string, *restic.Node, *repository.Repository) *bluge.Document
}

type FileDocumentBuilder struct{}

func (i FileDocumentBuilder) BuildDocument(fileID string, node *restic.Node, repo *repository.Repository) *bluge.Document {
	return bluge.NewDocument(fileID)
}
