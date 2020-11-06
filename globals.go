package rindex

import "errors"

var NilNodeFoundError = errors.New("nil node found")
var AlreadyIndexedError = errors.New("file already indexed")
