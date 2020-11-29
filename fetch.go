package rindex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/rubiojr/rapi"
	"github.com/rubiojr/rapi/repository"
	"github.com/rubiojr/rapi/restic"
)

func (i *Indexer) Fetch(ctx context.Context, fileID string, writer io.Writer) error {
	ropts := rapi.DefaultOptions
	ropts.Password = i.RepositoryPassword
	ropts.Repo = i.RepositoryLocation
	repo, err := rapi.OpenRepository(ropts)
	if err != nil {
		return err
	}

	var decodeError error
	var pblobs []restic.PackedBlob
	i.Search(fmt.Sprintf("_id:%s", fileID), func(field string, value []byte) bool {
		if field == "blobs" {
			decodeError = json.Unmarshal(value, &pblobs)
		}
		return true
	}, nil)

	if decodeError != nil {
		return fmt.Errorf("error unmarshalling blobs: %v", decodeError)
	}

	if len(pblobs) == 0 {
		return errors.Errorf("no blobs found for %s", fileID)
	}

	var buf []byte
	for _, blob := range pblobs {
		plaintext, err := loadBlob(ctx, repo, buf, blob)
		if err != nil {
			return err
		}
		writer.Write(plaintext)
	}

	return nil
}

// Retrieve and decrypt blob from repository.
// Similar to Restic's repository.LoadBlob but without using the in memory index, saving us tons of
// memory in large repos.
func loadBlob(ctx context.Context, r *repository.Repository, buf []byte, blob restic.PackedBlob) ([]byte, error) {
	// load blob from pack
	h := restic.Handle{Type: restic.PackFile, Name: blob.PackID.String()}

	switch {
	case cap(buf) < int(blob.Length):
		buf = make([]byte, blob.Length)
	case len(buf) != int(blob.Length):
		buf = buf[:blob.Length]
	}

	n, err := restic.ReadAt(ctx, r.Backend(), h, int64(blob.Offset), buf)
	if err != nil {
		return nil, err
	}

	if uint(n) != blob.Length {
		return nil, errors.Errorf("error loading blob %v: wrong length returned, want %d, got %d",
			blob.ID.Str(), blob.Length, uint(n))
	}

	// decrypt
	nonce, ciphertext := buf[:r.Key().NonceSize()], buf[r.Key().NonceSize():]
	plaintext, err := r.Key().Open(ciphertext[:0], nonce, ciphertext, nil)
	if err != nil {
		return nil, errors.Errorf("decrypting blob %v failed: %v", blob.ID, err)
	}

	// check hash
	if !restic.Hash(plaintext).Equal(blob.ID) {
		return nil, errors.Errorf("blob %v returned invalid hash", blob.ID)
	}

	// move decrypted data to the start of the buffer
	copy(buf, plaintext)
	return buf[:len(plaintext)], nil
}
