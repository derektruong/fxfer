package local

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

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

var defaultFilePerm = os.FileMode(0664)

type Destination struct {
	logger logr.Logger

	// bytesTransferred are used to store the destination bytes transferred
	bytesTransferred *int64
}

func NewDestination(logger logr.Logger) (s *Destination, err error) {
	s = &Destination{
		logger:           logger.WithName("local.destination"),
		bytesTransferred: new(int64),
	}
	if err = s.registerMeterCallback(); err != nil {
		return
	}
	return
}

func (d *Destination) Close() {
	d.logger.Info("closed local destination")
}

func (d *Destination) GetFileInfo(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (info xferfile.Info, err error) {
	if _, ok := cli.GetCredential().(local.IO); !ok {
		err = storage.ErrLocalProtocolIOInvalid
		return
	}

	// extract file info
	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(filePath); err != nil {
		return
	}

	// read file info
	var infoData []byte
	if infoData, err = os.ReadFile(infoPath); err != nil {
		if os.IsNotExist(err) {
			err = xferfile.ErrFileNotExists
		}
		return
	}
	if err = json.Unmarshal(infoData, &info); err != nil {
		return
	}

	// open file for reading
	var fileStat os.FileInfo
	if fileStat, err = os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			err = xferfile.ErrFileNotExists
			return
		}
		return
	}

	// set the file info offset
	info.Offset = fileStat.Size()
	return
}

func (d *Destination) CreateFile(
	ctx context.Context,
	path string, size int64, modTime time.Time,
	cli protoc.Client,
) (err error) {
	if _, ok := cli.GetCredential().(local.IO); !ok {
		err = storage.ErrLocalProtocolIOInvalid
		return
	}

	var dirPath, fileName, fileExt string
	if dirPath, fileName, fileExt, err = fileutils.ExtractFileParts(path); err != nil {
		return
	}

	// create directory if not exists
	if _, err = os.Stat(dirPath); os.IsNotExist(err) {
		if err = os.MkdirAll(dirPath, 0755); err != nil {
			return
		}
	}

	// open file with create flag
	var file *os.File
	if file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY, defaultFilePerm); err != nil {
		if os.IsNotExist(err) {
			err = xferfile.ErrFileNotExists
		}
		return
	}
	defer file.Close()

	return d.writeInfo(path, xferfile.Info{
		Path:      path,
		Size:      size,
		ModTime:   modTime,
		StartTime: time.Now(),
		Name:      fileName,
		Extension: fileExt,
	})
}

func (d *Destination) TransferFileChunk(
	ctx context.Context,
	filePath string,
	reader io.Reader,
	offset int64,
	cli protoc.Client,
) (n int64, err error) {
	if _, ok := cli.GetCredential().(local.IO); !ok {
		err = storage.ErrLocalProtocolIOInvalid
		return
	}

	// open file with append flag
	var file *os.File
	if file, err = os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, defaultFilePerm); err != nil {
		return
	}
	defer file.Close()
	transferReader := iometer.NewTransferReader(reader, d.bytesTransferred)
	defer transferReader.Close()
	if n, err = io.Copy(file, transferReader); err != nil {
		return
	}
	return
}

func (d *Destination) FinalizeTransfer(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (err error) {
	if _, ok := cli.GetCredential().(local.IO); !ok {
		err = storage.ErrLocalProtocolIOInvalid
		return
	}

	var info xferfile.Info
	if info, err = d.GetFileInfo(ctx, filePath, cli); err != nil {
		return
	}
	if info.Offset != info.Size {
		err = storage.ErrFileOrObjectCannotFinalize
		return
	}
	info.Offset = info.Size
	info.FinishTime = time.Now()
	return d.writeInfo(filePath, info)
}

func (d *Destination) DeleteFile(ctx context.Context, filePath string, protocol protoc.Client) (err error) {
	if _, ok := protocol.GetCredential().(local.IO); !ok {
		err = storage.ErrLocalProtocolIOInvalid
		return
	}

	if err = os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			err = xferfile.ErrFileNotExists
		}
		return
	}
	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(filePath); err != nil {
		return
	}
	if err = os.Remove(infoPath); err != nil {
		if os.IsNotExist(err) {
			err = xferfile.ErrFileNotExists
		}
		return
	}
	return
}

func (d *Destination) writeInfo(filePath string, info xferfile.Info) (err error) {
	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(filePath); err != nil {
		return
	}
	var infoData []byte
	if infoData, err = json.Marshal(info); err != nil {
		return
	}
	return os.WriteFile(infoPath, infoData, defaultFilePerm)
}

func (d *Destination) registerMeterCallback() (err error) {
	meter := otel.GetMeterProvider().Meter(fmt.Sprintf("%s/destination", meterNamePrefix))
	var totalBytesTransferred metric.Int64ObservableCounter
	if totalBytesTransferred, err = meter.Int64ObservableCounter("bytes_transferred"); err != nil {
		return
	}

	// setup observer
	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) (err error) {
			o.ObserveInt64(totalBytesTransferred, *d.bytesTransferred)
			return
		},
		totalBytesTransferred,
	)
	return
}
