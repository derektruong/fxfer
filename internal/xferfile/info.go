package xferfile

import (
	"errors"
	"fmt"
	"time"

	"github.com/derektruong/fxfer/internal/fileutils"
)

var ErrFileNotExists = errors.New("file path does not exist")

// Info represents information about the file transfer, this information is stored in the destination file
type Info struct {
	// Path is the path of the destination file
	Path string `json:"path"`

	// Size is the size of the file in bytes (source = destination)
	Size int64 `json:"size"`

	// Name contains the name of the destination file (without extension)
	Name string `json:"name"`

	// Extension contains the file extension of the file.
	Extension string `json:"extension"`

	// ModTime is the modification time of the source file (not destination file)
	// It is used to validate the file transfer integrity (not change during transfer)
	ModTime time.Time `json:"modTime"`

	// StartTime is the time the file transfer started (the time destination file was initiated)
	StartTime time.Time `json:"startTime"`

	// FinishTime is the time the file transfer finished (the time destination file was completed)
	FinishTime time.Time `json:"finishTime"`

	// Offset in bytes (zero-based), indicating the position of the last byte transferred
	Offset int64 `json:"offset"`

	// Checksum contains the checksum of the file (optional)
	Checksum []byte `json:"checksum,omitempty"`

	// Metadata contains additional information about the file (optional)
	Metadata map[string]string `json:"metadata,omitempty"`
}

// GenerateInfoPath generates the path of the info file based on the file path
func GenerateInfoPath(filePath string) (infoPath string, err error) {
	var prefix, fileName string
	if prefix, fileName, _, err = fileutils.ExtractFileParts(filePath); err != nil {
		return
	}
	infoPath = fmt.Sprintf("%s/%s.%s", prefix, fileName, "info")
	return
}
