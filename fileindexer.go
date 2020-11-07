package rindex

import (
	"github.com/blugelabs/bluge"
	"github.com/rubiojr/rapi/restic"
	"github.com/rubiojr/rindex/blugeindex"
)

type FileIndexer struct{}

func NewFileIndexer() *FileIndexer {
	return &FileIndexer{}
}

//TODO: not thread safe
func (i *FileIndexer) ShouldIndex(fileID string, bindex *blugeindex.BlugeIndex, node *restic.Node, repoId, repoLocation string) (*bluge.Document, bool) {
	doc := bluge.NewDocument(fileID).
		AddField(bluge.NewTextField("filename", string(node.Name)).StoreValue().HighlightMatches()).
		AddField(bluge.NewTextField("blobs", MarshalBlobIDs(node.Content)).StoreValue()).
		AddField(bluge.NewDateTimeField("mod_time", node.ModTime).StoreValue().HighlightMatches()).
		AddField(bluge.NewTextField("repository_location", repoLocation).StoreValue().HighlightMatches()).
		AddField(bluge.NewTextField("repository_id", repoId).StoreValue().HighlightMatches())

	return doc, true
}
