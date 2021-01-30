package rindex

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/rubiojr/rindex/internal/testutil"
)

func TestFetch(t *testing.T) {
	testutil.SetupRepo()

	progress := make(chan IndexStats, 10)
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
	if err != nil {
		t.Fatal(err)
	}

	stats, err := idx.Index(context.Background(), DefaultIndexOptions, progress)
	if err != nil {
		t.Error(err)
	}

	if stats.IndexedFiles == 0 {
		t.Fatal("should have indexed something")
	}

	fid := ""
	_, err = idx.Search("f22f05e5d1d07ab02a1c25f89d37b882855823257377313364351b9d2ca1cd22",
		func(field string, value []byte) bool {
			if field == "_id" {
				fid = string(value)
			}
			return true
		},
		func() bool { return true },
	)
	if err != nil {
		panic(err)
	}

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
	idx, err := New(testutil.IndexPath(), testutil.REPO_PATH, testutil.REPO_PASS)
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
		t.Errorf("invalid error returned: %+v", err)
	}
}
