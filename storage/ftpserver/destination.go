package ftpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"github.com/derektruong/fxfer/internal/fileutils"
	"github.com/derektruong/fxfer/internal/iometer"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/ftp"
	"github.com/derektruong/fxfer/storage"
)

type Destination struct {
	logger logr.Logger

	// connsMu and conns are used to store the destination connections
	connsMu sync.Mutex
	conns   map[string]protoc.ConnectionPool

	// bytesTransferredMu and bytesTransferred are used to store the destination bytes transferred
	bytesTransferredMu sync.RWMutex
	bytesTransferred   map[string]*int64

	// totalHosts is used to store the total number of ftp hosts connected
	totalHosts int32
}

func NewDestination(logger logr.Logger) (s *Destination) {
	s = &Destination{
		logger:           logger.WithName("ftp-server.destination"),
		conns:            make(map[string]protoc.ConnectionPool),
		bytesTransferred: make(map[string]*int64),
	}
	return
}

func (d *Destination) Close() {
	d.connsMu.Lock()
	defer d.connsMu.Unlock()
	for _, conn := range d.conns {
		conn.Close()
	}
	d.logger.Info("closed ftp-server destination")
}

func (d *Destination) GetFileInfo(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (info xferfile.Info, err error) {
	var conn protoc.ConnectionPool
	if conn, err = d.checkAndSetConnection(cli); err != nil {
		return
	}

	// get destination file size
	var size int64
	if size, _, err = conn.GetFileSizeAndModTime(ctx, filePath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}

	// extract file info
	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(filePath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}

	// open file for reading, this logic block must be after getting file size
	// to avoid 226 Transfer complete error
	var file io.ReadCloser
	if file, err = conn.RetrieveFileFromOffset(ctx, infoPath, io.SeekStart); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}
	defer file.Close()

	// read file info
	if err = json.NewDecoder(file).Decode(&info); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}

	// set the file info offset
	info.Offset = size
	return
}

func (d *Destination) CreateFile(
	ctx context.Context,
	path string, size int64, modTime time.Time,
	cli protoc.Client,
) (err error) {
	var conn protoc.ConnectionPool
	if conn, err = d.checkAndSetConnection(cli); err != nil {
		return
	}

	var dirPath, fileName, fileExt string
	if dirPath, fileName, fileExt, err = fileutils.ExtractFileParts(path); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}

	// create directory if not exists
	if err = conn.MakeDirectoryAll(ctx, dirPath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}

	// create file holder if not exists
	if err = conn.CreateOrOverwriteFile(ctx, path, bytes.NewReader([]byte{})); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}

	// create file info
	fileInfo := xferfile.Info{
		Path:      path,
		Size:      size,
		ModTime:   modTime,
		StartTime: time.Now(),
		Name:      fileName,
		Extension: fileExt,
	}
	if err = d.writeInfo(ctx, conn, path, fileInfo); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}

	return
}

func (d *Destination) TransferFileChunk(
	ctx context.Context,
	filePath string,
	reader io.Reader,
	offset int64,
	cli protoc.Client,
) (n int64, err error) {
	var conn protoc.ConnectionPool
	if conn, err = d.checkAndSetConnection(cli); err != nil {
		return
	}
	connID := cli.GetConnectionID()
	transferred := d.getBytesTransferred(connID)
	if transferred == nil {
		d.setBytesTransferred(connID)
		transferred = d.getBytesTransferred(connID)
	}
	transferReader := iometer.NewTransferReader(reader, transferred)
	defer transferReader.Close()
	if err = conn.AppendToFile(ctx, filePath, transferReader, offset); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}
	return
}

func (d *Destination) FinalizeTransfer(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (err error) {
	var conn protoc.ConnectionPool
	if conn, err = d.checkAndSetConnection(cli); err != nil {
		return
	}
	var info xferfile.Info
	if info, err = d.GetFileInfo(ctx, filePath, cli); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}
	if info.Offset != info.Size {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, storage.ErrFileOrObjectCannotFinalize)
		return
	}
	info.Offset = info.Size
	info.FinishTime = time.Now()
	if err = d.writeInfo(ctx, conn, filePath, info); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}
	return
}

func (d *Destination) DeleteFile(ctx context.Context, filePath string, cli protoc.Client) (err error) {
	var conn protoc.ConnectionPool
	if conn, err = d.checkAndSetConnection(cli); err != nil {
		return
	}
	if err = conn.DeleteFile(ctx, filePath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}
	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(filePath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}
	if err = conn.DeleteFile(ctx, infoPath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, false, err)
		return
	}
	return
}

func (d *Destination) writeInfo(
	ctx context.Context,
	conn protoc.ConnectionPool,
	filePath string,
	info xferfile.Info,
) (err error) {
	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(filePath); err != nil {
		return
	}
	var infoData []byte
	if infoData, err = json.Marshal(info); err != nil {
		return
	}
	err = conn.CreateOrOverwriteFile(ctx, infoPath, bytes.NewReader(infoData))
	return
}

func (d *Destination) checkAndSetConnection(cli protoc.Client) (conn protoc.ConnectionPool, err error) {
	d.connsMu.Lock()
	defer d.connsMu.Unlock()
	connID := cli.GetConnectionID()
	var exists bool
	if conn, exists = d.conns[connID]; !exists {
		// setup connection
		var connPool protoc.ConnectionPool
		connPool = cli.GetConnectionPool(d.logger)
		cred, ok := cli.GetCredential().(ftp.Client)
		if !ok {
			err = protoc.ErrProtocolInvalid(protoc.FTPProtoc)
			return
		}
		if err = connPool.InitializeIdleConnection(cli.GetCredential()); err != nil {
			return
		}
		conn = connPool
		d.conns[connID] = connPool

		// setup meter
		if err = d.setupMetricMeter(connID, cred); err != nil {
			return
		}
	}
	return
}

func (d *Destination) setupMetricMeter(connID string, credential ftp.Client) (err error) {
	// setup meter
	meter := otel.GetMeterProvider().Meter(
		fmt.Sprintf("%s/destination/%s", meterNamePrefix, credential.GetAddress()),
	)

	// setup bytes transferred
	d.bytesTransferredMu.Lock()
	defer d.bytesTransferredMu.Unlock()
	d.bytesTransferred[connID] = new(int64)

	// setup total hosts
	atomic.AddInt32(&d.totalHosts, 1)

	var totalBytesTransferred metric.Int64ObservableCounter
	if totalBytesTransferred, err = meter.Int64ObservableCounter("bytes_transferred"); err != nil {
		return
	}

	var totalHosts metric.Int64ObservableGauge
	if totalHosts, err = meter.Int64ObservableGauge("total_connected_host"); err != nil {
		return
	}

	// setup observer
	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) (err error) {
			o.ObserveInt64(totalHosts, int64(d.totalHosts))
			o.ObserveInt64(totalBytesTransferred, *d.bytesTransferred[connID])
			return
		},
		totalHosts,
		totalBytesTransferred,
	)
	return
}

func (d *Destination) getBytesTransferred(connID string) (transferred *int64) {
	d.bytesTransferredMu.RLock()
	defer d.bytesTransferredMu.RUnlock()
	var exists bool
	if transferred, exists = d.bytesTransferred[connID]; !exists {
		return nil
	}
	return
}

func (d *Destination) setBytesTransferred(connID string) {
	d.bytesTransferredMu.Lock()
	defer d.bytesTransferredMu.Unlock()
	d.bytesTransferred[connID] = new(int64)
	return
}
