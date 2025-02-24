package fxfer

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"

	"github.com/derektruong/fxfer/internal/xferfile"

	"github.com/derektruong/fxfer/internal/sliceutils"
)

var (
	ErrMaxFileSizeExceeded = func(required, got int64) error {
		return fmt.Errorf("file size exceeds the maximum allowed size: %d > %d bytes", got, required)
	}
	ErrMinFileSizeNotMet = func(required, got int64) error {
		return fmt.Errorf("file size does not meet the minimum required size: %d < %d bytes", got, required)
	}
	ErrExtensionNotAllowed = func(ext string) error {
		return fmt.Errorf("file extension is not allowed: %s", ext)
	}
	ErrExtensionBlocked = func(ext string) error {
		return fmt.Errorf("file extension is blocked: %s", ext)
	}
	ErrModifiedBefore = func(t time.Time) error {
		return fmt.Errorf("file was modified before the required time: %s", t.Format(time.RFC3339))
	}
	ErrModifiedAfter = func(t time.Time) error {
		return fmt.Errorf("file was modified after the required time: %s", t.Format(time.RFC3339))
	}
	ErrFileNamePatternMismatch = func(pattern string) error {
		return fmt.Errorf("file name does not match the required pattern: %s", pattern)
	}
)

// fileRule defines the rules for file transfer
type fileRule struct {
	// MaxFileSize allows setting a maximum file size for transfer.
	MaxFileSize int64
	// MinFileSize allows setting a minimum file size for transfer.
	MinFileSize int64
	// ExtensionWhitelist allows setting a list of allowed file extensions.
	ExtensionWhitelist []string
	// ExtensionBlacklist allows setting a list of blocked file extensions.
	ExtensionBlacklist []string
	// ModifiedAfter allows setting a minimum modified time for transfer.
	ModifiedAfter time.Time
	// ModifiedBefore allows setting a maximum modified time for transfer.
	ModifiedBefore time.Time
	// FileNamePattern allows setting a regular expression pattern for file names.
	FileNamePattern *regexp.Regexp
}

func (r *fileRule) Check(fileInfo xferfile.Info) (err error) {
	// check file size
	if r.MaxFileSize > 0 && fileInfo.Size > r.MaxFileSize {
		return ErrMaxFileSizeExceeded(r.MaxFileSize, fileInfo.Size)
	}
	if r.MinFileSize > 0 && fileInfo.Size < r.MinFileSize {
		return ErrMinFileSizeNotMet(r.MinFileSize, fileInfo.Size)
	}

	// check file extension
	if len(r.ExtensionWhitelist) > 0 && !sliceutils.Contains(r.ExtensionWhitelist, fileInfo.Extension) {
		return ErrExtensionNotAllowed(fileInfo.Extension)
	}
	if len(r.ExtensionBlacklist) > 0 && sliceutils.Contains(r.ExtensionBlacklist, fileInfo.Extension) {
		return ErrExtensionBlocked(fileInfo.Extension)
	}

	// check modified time
	if !r.ModifiedAfter.IsZero() &&
		fileInfo.ModTime.Before(r.ModifiedAfter) {
		return ErrModifiedAfter(r.ModifiedAfter)
	}
	if !r.ModifiedBefore.IsZero() &&
		fileInfo.ModTime.After(r.ModifiedBefore) {
		return ErrModifiedBefore(r.ModifiedBefore)
	}

	// check file name pattern
	if r.FileNamePattern != nil &&
		!r.FileNamePattern.MatchString(filepath.Base(fileInfo.Path)) {
		return ErrFileNamePatternMismatch(r.FileNamePattern.String())
	}
	return
}
