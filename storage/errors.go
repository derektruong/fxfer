package storage

import (
	"errors"
	"fmt"
)

type Storage string

const (
	S3Storage         Storage = "S3"
	FTPServerStorage  Storage = "FTPServer"
	SFTPServerStorage Storage = "SFTPServer"
	LocalStorage      Storage = "Local"
)

var ErrFileOrObjectCannotFinalize = errors.New("file or object cannot finalize, retry if possible")

// WrapErrorWithStorage wraps the error with storage information
//
// Parameters:
//   - storage: The type of storage (e.g., S3, FTPServer, etc.)
//   - isSource: Indicates whether the storage is a source or destination
//   - err: The original error to be wrapped
//
// Returns:
//   - error: The verbose error message with storage information
func WrapErrorWithStorage(storage Storage, isSource bool, err error) error {
	if err == nil {
		return nil
	}
	var storageType string
	if isSource {
		storageType = "source"
	} else {
		storageType = "destination"
	}
	return fmt.Errorf("at %q %s storage, occurred error: %w", storage, storageType, err)
}
