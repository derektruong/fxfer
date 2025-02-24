package local

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/derektruong/fxfer/internal/fileutils"
	"github.com/derektruong/fxfer/internal/iometer"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/local"
	"github.com/derektruong/fxfer/storage"
	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const meterNamePrefix = "transferer/storage/local"

type Source struct {
	logger logr.Logger

	// bytesTransferred are used to store the destination bytes transferred
	bytesTransferred *int64
}

func NewSource(logger logr.Logger) (s *Source, err error) {
	s = &Source{
		logger:           logger.WithName("local.source"),
		bytesTransferred: new(int64),
	}
	if err = s.registerMeterCallback(); err != nil {
		return
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
	reader = iometer.NewTransferReader(file, s.bytesTransferred)
	return
}

func (s *Source) Close() {
	s.logger.Info("closed local source")
}

func (s *Source) registerMeterCallback() (err error) {
	meter := otel.GetMeterProvider().Meter(fmt.Sprintf("%s/source", meterNamePrefix))
	var totalBytesTransferred metric.Int64ObservableCounter
	if totalBytesTransferred, err = meter.Int64ObservableCounter("bytes_transferred"); err != nil {
		return
	}

	// setup observer
	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) (err error) {
			o.ObserveInt64(totalBytesTransferred, *s.bytesTransferred)
			return
		},
		totalBytesTransferred,
	)
	return
}
