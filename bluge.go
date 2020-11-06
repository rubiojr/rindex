package rindex

import (
	"github.com/blugelabs/bluge"
)

var bWriter *bluge.Writer

func blugeReader(indexPath string) *bluge.Reader {
	r, err := blugeWriter(indexPath).Reader()
	if err != nil {
		panic(err)
	}
	return r
}

func blugeWriter(indexPath string) *bluge.Writer {
	var err error
	if bWriter == nil {
		blugeConf := bluge.DefaultConfig(indexPath)
		bWriter, err = bluge.OpenWriter(blugeConf)
		if err != nil {
			panic(err)
		}
	}

	return bWriter
}
