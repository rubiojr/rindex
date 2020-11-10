package rindex

import (
	"mime"
	"path/filepath"

	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rindex/blugeindex"
)

type FileIndexer struct{}

func NewFileIndexer() *FileIndexer {
	return &FileIndexer{}
}

func (i *FileIndexer) ShouldIndex(fileID string, bindex *blugeindex.BlugeIndex, node *restic.Node, repo *repository.Repository) (*bluge.Document, bool) {
	doc := bluge.NewDocument(fileID).
		AddField(bluge.NewTextField("mime_type", mime.TypeByExtension(filepath.Ext(node.Name))).StoreValue().HighlightMatches())

	return doc, true
}
