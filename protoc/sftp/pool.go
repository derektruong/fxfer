package sftp

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/derektruong/fxfer/protoc"
	"github.com/go-logr/logr"
	"github.com/jackc/puddle/v2"
	"github.com/pkg/sftp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/crypto/ssh"
)

const otelInstrumentPrefix = "transferer/protoc/sftp"

type sftpConnection struct {
	sshClient  *ssh.Client
	sftpClient *sftp.Client
	maxAgeTime time.Time
}

func (c *sftpConnection) isExpired(res *puddle.Resource[*sftpConnection]) bool {
	return time.Now().After(res.Value().maxAgeTime)
}

type connectionPool struct {
	logger logr.Logger

	// mu is a mutex to protect the connection pool
	connPool *puddle.Pool[*sftpConnection]

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
		logger:            logger.WithName("sftp.connectionPool"),
		connPool:          new(puddle.Pool[*sftpConnection]),
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
	p.logger.Info("SFTP Connection pool is closed")
}

func (p *connectionPool) InitializeIdleConnection(config any) (err error) {
	sftpConfig, ok := config.(Client)
	if !ok {
		err = protoc.ErrClientConfigInvalid(protoc.SFTPProtoc)
		return
	}
	constructor := func(ctx context.Context) (conn *sftpConnection, err error) {
		// create SSH credentials config
		cfg := &ssh.ClientConfig{
			Timeout:         1 * time.Hour,
			User:            sftpConfig.Username,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			Auth:            []ssh.AuthMethod{ssh.Password(sftpConfig.Password)},
		}
		// create SSH client
		var sshClient *ssh.Client
		if sshClient, err = ssh.Dial("tcp", sftpConfig.GetAddress(), cfg); err != nil {
			return
		}
		// create SFTP client
		var sftpClient *sftp.Client
		if sftpClient, err = sftp.NewClient(sshClient); err != nil {
			if err = sshClient.Close(); err != nil {
				p.logger.Error(err, "failed to close SSH client")
			}
			return
		}
		conn = &sftpConnection{
			sshClient:  sshClient,
			sftpClient: sftpClient,
			maxAgeTime: time.Now().Add(p.maxConnLifetime),
		}
		return
	}
	destructor := func(conn *sftpConnection) {
		if err = conn.sftpClient.Close(); err != nil {
			p.logger.Error(err, "failed to close SFTP client")
		}
		if err = conn.sshClient.Close(); err != nil {
			p.logger.Error(err, "failed to close SSH client")
		}
	}
	var pool *puddle.Pool[*sftpConnection]
	if pool, err = puddle.NewPool(&puddle.Config[*sftpConnection]{
		Constructor: constructor,
		Destructor:  destructor,
		MaxSize:     p.maxPoolSize,
	}); err != nil {
		return
	}
	if err = p.registerMeterCallback(sftpConfig.GetAddress()); err != nil {
		return
	}
	p.connPool = pool
	return
}

func (p *connectionPool) GetFileSizeAndModTime(
	ctx context.Context,
	filePath string,
) (size int64, modTime time.Time, err error) {
	var resource *puddle.Resource[*sftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	var fileInfo os.FileInfo
	if fileInfo, err = conn.sftpClient.Stat(filePath); err != nil {
		return
	}
	size = fileInfo.Size()
	modTime = fileInfo.ModTime()
	return
}

func (p *connectionPool) RetrieveFileFromOffset(
	ctx context.Context,
	filePath string,
	offset int64,
) (reader io.ReadCloser, err error) {
	var resource *puddle.Resource[*sftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	var file *sftp.File
	if file, err = conn.sftpClient.Open(filePath); err != nil {
		return
	}
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return
	}
	reader = file
	return
}

func (p *connectionPool) MakeDirectoryAll(ctx context.Context, dirPath string) (err error) {
	var resource *puddle.Resource[*sftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	if err = conn.sftpClient.MkdirAll(dirPath); err != nil {
		return
	}
	return
}

func (p *connectionPool) CreateOrOverwriteFile(
	ctx context.Context,
	filePath string,
	reader io.Reader,
) (err error) {
	var resource *puddle.Resource[*sftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	var file *sftp.File
	if file, err = conn.sftpClient.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY); err != nil {
		return
	}
	defer file.Close()
	if _, err = io.Copy(file, reader); err != nil {
		return
	}
	return
}

func (p *connectionPool) AppendToFile(
	ctx context.Context,
	filePath string, reader io.Reader, offset int64,
) (err error) {
	var resource *puddle.Resource[*sftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	var file *sftp.File
	if file, err = conn.sftpClient.OpenFile(filePath, os.O_APPEND|os.O_WRONLY); err != nil {
		return
	}
	defer file.Close()
	if _, err = file.Seek(offset, io.SeekStart); err != nil {
		return
	}
	if _, err = io.Copy(file, reader); err != nil {
		return
	}
	return
}

func (p *connectionPool) DeleteFile(ctx context.Context, filePath string) (err error) {
	var resource *puddle.Resource[*sftpConnection]
	if resource, err = p.connPool.Acquire(ctx); err != nil {
		return
	}
	defer resource.Release()
	conn := resource.Value()
	if err = conn.sftpClient.Remove(filePath); err != nil {
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

	totalConns, err := meter.Int64ObservableGauge("sftppool_total_connections")
	if err != nil {
		return
	}

	acquireCount, err := meter.Int64ObservableGauge("sftppool_acquire_count")
	if err != nil {
		return
	}

	acquireDuration, err := meter.Int64ObservableGauge("sftppool_acquire_duration")
	if err != nil {
		return
	}

	acquiredConns, err := meter.Int64ObservableGauge("sftppool_acquired_connections")
	if err != nil {
		return
	}

	canceledAcquireCount, err := meter.Int64ObservableGauge("sftppool_canceled_acquire_count")
	if err != nil {
		return
	}

	constructingConns, err := meter.Int64ObservableGauge("sftppool_waiting_connections")
	if err != nil {
		return
	}

	emptyAcquireCount, err := meter.Int64ObservableGauge("sftppool_empty_acquire_count")
	if err != nil {
		return
	}

	idleConns, err := meter.Int64ObservableGauge("sftppool_idle_connections")
	if err != nil {
		return
	}

	maxConns, err := meter.Int64ObservableGauge("sftppool_max_connections")
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
