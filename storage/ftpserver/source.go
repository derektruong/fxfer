package ftpserver

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"io"
	"github.com/derektruong/fxfer/internal/fileutils"
	"github.com/derektruong/fxfer/internal/iometer"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/ftp"
	"github.com/derektruong/fxfer/storage"
	"sync"
	"sync/atomic"
	"time"
)

const meterNamePrefix = "transferer/storage/ftp-server"

type Source struct {
	logger logr.Logger

	// connsMu and conns are used to store the destination connections by connection ID
	connsMu sync.Mutex
	conns   map[string]protoc.ConnectionPool

	// bytesTransferredMu and bytesTransferred are used to read from source bytes
	// transferred by connection ID
	bytesTransferredMu sync.RWMutex
	bytesTransferred   map[string]*int64

	// totalHosts is used to store the total number of ftp hosts connected
	totalHosts int32
}

func NewSource(logger logr.Logger) (s *Source) {
	s = &Source{
		logger:           logger.WithName("ftp-server.source"),
		conns:            make(map[string]protoc.ConnectionPool),
		bytesTransferred: make(map[string]*int64),
	}
	return
}

func (s *Source) Close() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.logger.Info("closed ftp-server source")
}

func (s *Source) GetFileInfo(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (info xferfile.Info, err error) {
	var conn protoc.ConnectionPool
	if conn, err = s.checkAndSetConnection(cli); err != nil {
		return
	}
	var size int64
	var modTime time.Time
	if size, modTime, err = conn.GetFileSizeAndModTime(ctx, filePath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, true, err)
		return
	}
	var fileName, fileExt string
	if _, fileName, fileExt, err = fileutils.ExtractFileParts(filePath); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, true, err)
		return
	}
	info = xferfile.Info{
		Path:      filePath,
		Size:      size,
		Name:      fileName,
		Extension: fileExt,
		ModTime:   modTime,
	}
	return
}

func (s *Source) GetFileFromOffset(
	ctx context.Context,
	filePath string,
	offset int64,
	cli protoc.Client,
) (reader io.ReadCloser, err error) {
	var conn protoc.ConnectionPool
	if conn, err = s.checkAndSetConnection(cli); err != nil {
		return
	}
	var fileReader io.ReadCloser
	if fileReader, err = conn.RetrieveFileFromOffset(ctx, filePath, offset); err != nil {
		err = storage.WrapErrorWithStorage(storage.FTPServerStorage, true, err)
		return
	}
	connID := cli.GetConnectionID()
	transferred := s.getBytesTransferred(connID)
	if transferred == nil {
		s.setBytesTransferred(connID)
		transferred = s.getBytesTransferred(connID)
	}
	reader = iometer.NewTransferReader(fileReader, transferred)
	return
}

func (s *Source) checkAndSetConnection(cli protoc.Client) (conn protoc.ConnectionPool, err error) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	connID := cli.GetConnectionID()
	var exists bool
	if conn, exists = s.conns[connID]; !exists {
		// setup connection\
		var connPool protoc.ConnectionPool
		connPool = cli.GetConnectionPool(s.logger)
		cred, ok := cli.GetCredential().(ftp.Client)
		if !ok {
			err = protoc.ErrProtocolInvalid(protoc.FTPProtoc)
			return
		}
		if err = connPool.InitializeIdleConnection(cli.GetCredential()); err != nil {
			return
		}
		conn = connPool
		s.conns[connID] = connPool

		// setup meter
		if err = s.setupMetricMeter(connID, cred); err != nil {
			return
		}
	}
	return
}

func (s *Source) setupMetricMeter(connID string, credential ftp.Client) (err error) {
	// setup meter
	meter := otel.GetMeterProvider().Meter(fmt.Sprintf("%s/source/%s", meterNamePrefix, credential.GetAddress()))

	// setup bytes transferred
	s.bytesTransferredMu.Lock()
	defer s.bytesTransferredMu.Unlock()
	s.bytesTransferred[connID] = new(int64)

	// setup total hosts
	atomic.AddInt32(&s.totalHosts, 1)

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
			o.ObserveInt64(totalHosts, int64(s.totalHosts))
			o.ObserveInt64(totalBytesTransferred, *s.bytesTransferred[connID])
			return
		},
		totalHosts,
		totalBytesTransferred,
	)
	return
}

func (s *Source) getBytesTransferred(connID string) (transferred *int64) {
	s.bytesTransferredMu.RLock()
	defer s.bytesTransferredMu.RUnlock()
	var exists bool
	if transferred, exists = s.bytesTransferred[connID]; !exists {
		return nil
	}
	return
}

func (s *Source) setBytesTransferred(connID string) {
	s.bytesTransferredMu.Lock()
	defer s.bytesTransferredMu.Unlock()
	s.bytesTransferred[connID] = new(int64)
	return
}
