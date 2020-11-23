package rindex

import (
	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rindex/blugeindex"
)

type FileDocumentBuilder struct{}

func (i FileDocumentBuilder) BuildDocument(fileID string, bindex blugeindex.BlugeIndex, node *restic.Node, repo *repository.Repository) *bluge.Document {
	return bluge.NewDocument(fileID)
}
