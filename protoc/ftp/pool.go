package ftp

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/go-logr/logr"
	"github.com/jackc/puddle/v2"
	goftp "github.com/jlaffaye/ftp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const otelInstrumentPrefix = "transferer/protoc/ftp"

const (
	err226TransferComplete = "226 Transfer complete"
)

type ftpConnection struct {
	serverConn *goftp.ServerConn
	maxAgeTime time.Time
}

func (c *ftpConnection) isExpired(res *puddle.Resource[*ftpConnection]) bool {
	return time.Now().After(res.Value().maxAgeTime)
}

type connectionPool struct {
	logger logr.Logger

	// mu is a mutex to protect the connection pool
	connPool *puddle.Pool[*ftpConnection]

	// doneCtx is a context to signal the background health check to stop
	doneCtx context.Context
	done    context.CancelFunc

	// config for pool
	maxPoolSize       int32
	maxConnIdleTime   time.Duration
	maxConnLifetime   time.Duration
	healthCheckPeriod time.Duration
}

func newConnectionPool(
	logger logr.Logger,
	maxPoolSize int32,
	maxConnIdleTime time.Duration,
	maxConnLifetime time.Duration,
	healthCheckPeriod time.Duration,
) (p *connectionPool) {
	p = &connectionPool{
		logger:            logger.WithName("ftp.connectionPool"),
		connPool:          new(puddle.Pool[*ftpConnection]),
		maxPoolSize:       maxPoolSize,
		maxConnIdleTime:   maxConnIdleTime,
		maxConnLifetime:   maxConnLifetime,
		healthCheckPeriod: healthCheckPeriod,
	}
	p.doneCtx, p.done = context.WithCancel(context.Background())
	go p.backgroundHealthCheck()
	return
}

func (p *connectionPool) Close() {
	p.done()
	p.connPool.Close()
	p.logger.Info("FTP Connection pool is closed")
}

func (p *connectionPool) InitializeIdleConnection(config any) (err error) {
	ftpConfig, ok := config.(Client)
	if !ok {
		err = protoc.ErrClientConfigInvalid(protoc.FTPProtoc)
		return
	}
	constructor := func(ctx context.Context) (conn *ftpConnection, err error) {
		var serverConn *goftp.ServerConn
		if serverConn, err = goftp.Dial(
			ftpConfig.GetAddress(),
			goftp.DialWithContext(ctx),
			goftp.DialWithTimeout(1*time.Hour),
			goftp.DialWithWritingMDTM(false),
			goftp.DialWithDisabledEPSV(true),
		); err != nil {
			return
		}
		if err = serverConn.Login(
			ftpConfig.Username,
			ftpConfig.Password,
		); err != nil {
			return
		}
		conn = &ftpConnection{
			serverConn: serverConn,
			maxAgeTime: time.Now().Add(p.maxConnLifetime),
		}
		return
	}
	destructor := func(conn *ftpConnection) {
		_ = conn.serverConn.Quit()
	}
	var pool *puddle.Pool[*ftpConnection]
	if pool, err = puddle.NewPool(&puddle.Config[*ftpConnection]{
		Constructor: constructor,
		Destructor:  destructor,
		MaxSize:     p.maxPoolSize,
	}); err != nil {
		return
	}
	if err = p.registerMeterCallback(ftpConfig.GetAddress()); err != nil {
		return
	}
	p.connPool = pool
	return
}

func (p *connectionPool) GetFileSizeAndModTime(
	ctx context.Context,
	filePath string,
) (size int64, modTime time.Time, err error) {
	var resource *puddle.Resource[*ftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	if size, err = conn.serverConn.FileSize(filePath); err != nil {
		if strings.HasPrefix(err.Error(), strconv.Itoa(goftp.StatusFileUnavailable)) {
			err = xferfile.ErrFileNotExists
		}
		if strings.Contains(err.Error(), err226TransferComplete) {
			err = nil
		}
		return
	}
	if modTime, err = conn.serverConn.GetTime(filePath); err != nil {
		return
	}
	return
}

func (p *connectionPool) RetrieveFileFromOffset(
	ctx context.Context,
	filePath string,
	offset int64,
) (reader io.ReadCloser, err error) {
	var resource *puddle.Resource[*ftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	var resp *goftp.Response
	if resp, err = conn.serverConn.RetrFrom(filePath, uint64(offset)); err != nil {
		if strings.HasPrefix(err.Error(), strconv.Itoa(goftp.StatusFileUnavailable)) {
			err = xferfile.ErrFileNotExists
		}
		return
	}
	reader = resp
	return
}

func (p *connectionPool) MakeDirectoryAll(ctx context.Context, dirPath string) (err error) {
	var resource *puddle.Resource[*ftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	// split the path into directories
	dirParts := strings.Split(dirPath, "/")
	if len(dirParts) == 0 {
		return
	}
	var walkDir string
	if dirParts[0] == "" {
		walkDir = "/"
		dirParts = dirParts[1:]
	}
	for index, dirPart := range dirParts {
		if index == 0 && walkDir == "" {
			walkDir = dirPart
		} else {
			walkDir = filepath.Join(walkDir, dirPart)
		}
		if err = conn.serverConn.MakeDir(walkDir); err != nil {
			// if the directory already exists, or failed to create the directory (550),
			// ignore the error
			if !strings.HasPrefix(err.Error(), strconv.Itoa(goftp.StatusFileUnavailable)) {
				return
			}
			err = nil
		}
	}
	return
}

func (p *connectionPool) CreateOrOverwriteFile(
	ctx context.Context,
	filePath string,
	reader io.Reader,
) (err error) {
	var resource *puddle.Resource[*ftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	// create directory if not exists
	if err = conn.serverConn.Stor(filePath, reader); err != nil {
		return
	}
	return
}

func (p *connectionPool) AppendToFile(
	ctx context.Context,
	filePath string,
	reader io.Reader,
	offset int64,
) (err error) {
	var resource *puddle.Resource[*ftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	// create directory if not exists
	if err = conn.serverConn.Append(filePath, reader); err != nil {
		return
	}
	return
}

func (p *connectionPool) DeleteFile(ctx context.Context, filePath string) (err error) {
	var resource *puddle.Resource[*ftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	if err = conn.serverConn.Delete(filePath); err != nil {
		return
	}
	return
}

func (p *connectionPool) backgroundHealthCheck() {
	timer := time.NewTimer(p.healthCheckPeriod)
	defer timer.Stop()
	for {
		select {
		case <-p.doneCtx.Done():
			return
		case <-timer.C:
			if destroyed := func() (isDestroyed bool) {
				totalResources := p.connPool.Stat().TotalResources()
				if totalResources == 0 {
					return
				}
				resources := p.connPool.AcquireAllIdle()
				for _, res := range resources {
					conn := res.Value()
					if conn.isExpired(res) || res.IdleDuration() > p.maxConnIdleTime {
						res.Destroy()
						isDestroyed = true
					} else {
						res.ReleaseUnused()
					}
				}
				return
			}(); destroyed {
				p.logger.Info(fmt.Sprintf(
					"some resources of connection is destroyed due to health check",
				))
			}
			timer.Reset(p.healthCheckPeriod)
		}
	}
}

func (p *connectionPool) registerMeterCallback(addr string) (err error) {
	meter := otel.GetMeterProvider().Meter(fmt.Sprintf("%s/%s", otelInstrumentPrefix, addr))

	totalConns, err := meter.Int64ObservableGauge("ftppool_total_connections")
	if err != nil {
		return
	}

	acquireCount, err := meter.Int64ObservableGauge("ftppool_acquire_count")
	if err != nil {
		return
	}

	acquireDuration, err := meter.Int64ObservableGauge("ftppool_acquire_duration")
	if err != nil {
		return
	}

	acquiredConns, err := meter.Int64ObservableGauge("ftppool_acquired_connections")
	if err != nil {
		return
	}

	canceledAcquireCount, err := meter.Int64ObservableGauge("ftppool_canceled_acquire_count")
	if err != nil {
		return
	}

	constructingConns, err := meter.Int64ObservableGauge("ftppool_waiting_connections")
	if err != nil {
		return
	}

	emptyAcquireCount, err := meter.Int64ObservableGauge("ftppool_empty_acquire_count")
	if err != nil {
		return
	}

	idleConns, err := meter.Int64ObservableGauge("ftppool_idle_connections")
	if err != nil {
		return
	}

	maxConns, err := meter.Int64ObservableGauge("ftppool_max_connections")
	if err != nil {
		return
	}

	// setup observer
	_, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) (err error) {
			o.ObserveInt64(totalConns, int64(p.connPool.Stat().TotalResources()))
			o.ObserveInt64(acquireCount, p.connPool.Stat().AcquireCount())
			o.ObserveInt64(acquireDuration, p.connPool.Stat().AcquireDuration().Milliseconds())
			o.ObserveInt64(acquiredConns, int64(p.connPool.Stat().AcquiredResources()))
			o.ObserveInt64(canceledAcquireCount, p.connPool.Stat().CanceledAcquireCount())
			o.ObserveInt64(constructingConns, int64(p.connPool.Stat().ConstructingResources()))
			o.ObserveInt64(emptyAcquireCount, p.connPool.Stat().EmptyAcquireCount())
			o.ObserveInt64(idleConns, int64(p.connPool.Stat().IdleResources()))
			o.ObserveInt64(maxConns, int64(p.connPool.Stat().MaxResources()))
			return
		},
		totalConns,
		acquireCount,
		acquireDuration,
		acquiredConns,
		canceledAcquireCount,
		constructingConns,
		emptyAcquireCount,
		idleConns,
		maxConns,
	)
	return
}
