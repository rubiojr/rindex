package rindex

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestFetch(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(indexPath(), os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}

	fid := ""
	idx.Search("7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730",
		func(field string, value []byte) bool {
			if field == "_id" {
				fid = string(value)
			}
			return true
		},
		func() bool { return true },
	)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	err = idx.Fetch(context.Background(), fid, writer)
	writer.Flush()
	if err != nil {
		t.Error(err)
	}

	if strings.TrimSpace(buf.String()) != "bar" {
		t.Errorf("%s != %s", buf.String(), "bar")
	}

	sha := fmt.Sprintf("%x", sha256.Sum256(buf.Bytes()))
	// file testdata/bar SHA256
	if sha != "7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730" {
		t.Error("sha256 does not match after decryption")
	}
}

func TestFetchInvalid(t *testing.T) {
	progress := make(chan IndexStats, 10)
	idx, err := New(indexPath(), os.Getenv("RESTIC_REOPOSITORY"), os.Getenv("RESTIC_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	err = idx.Fetch(context.Background(), "XXX", writer)
	writer.Flush()
	if err == nil {
		t.Error("should have returned an error")
	}

	if err.Error() != "no blobs found for XXX" {
		t.Error("invalid error returned")
	}
}
