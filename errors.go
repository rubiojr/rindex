package rindex

import "errors"

var ErrSearchNotReady = errors.New("not ready for search")
var ErrIndexLocked = errors.New("there's an Indexer instance running")
