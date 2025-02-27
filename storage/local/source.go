package local

import (
	"context"
	"io"
	"os"

	"github.com/derektruong/fxfer/internal/fileutils"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/local"
	"github.com/derektruong/fxfer/storage"
	"github.com/go-logr/logr"
)

type Source struct {
	logger logr.Logger
}

func NewSource(logger logr.Logger) (s *Source, err error) {
	s = &Source{
		logger: logger.WithName("local.source"),
	}
	return
}

func (s *Source) GetFileInfo(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (info xferfile.Info, err error) {
	if _, ok := cli.GetCredential().(local.IO); !ok {
		err = storage.ErrLocalProtocolIOInvalid
		return
	}
	var fileInfo os.FileInfo
	if fileInfo, err = os.Stat(filePath); err != nil {
		return
	}
	var fileName, fileExt string
	if _, fileName, fileExt, err = fileutils.ExtractFileParts(filePath); err != nil {
		return
	}
	info = xferfile.Info{
		Path:      filePath,
		Name:      fileName,
		Extension: fileExt,
		Size:      fileInfo.Size(),
		ModTime:   fileInfo.ModTime(),
	}
	return
}

func (s *Source) GetFileFromOffset(
	ctx context.Context,
	filePath string,
	offset int64,
	cli protoc.Client,
) (reader io.ReadCloser, err error) {
	if _, ok := cli.GetCredential().(local.IO); !ok {
		err = storage.ErrLocalProtocolIOInvalid
		return
	}
	var file *os.File
	if file, err = os.Open(filePath); err != nil {
		return
	}
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return
	}
	reader = file
	return
}

func (s *Source) Close() {
	s.logger.Info("closed local source")
}
