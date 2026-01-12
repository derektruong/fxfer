package ftp

import (
	"errors"
	"fmt"
	"time"

	"github.com/derektruong/fxfer/internal/protocutils"
	"github.com/derektruong/fxfer/protoc"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
)

var connectionIDNamespace = uuid.MustParse("344e4af6-34c9-459f-9e2e-0cad8b451034")

const (
	defaultMaxPoolSize       = int32(20)
	defaultMaxConnIdleTime   = 5 * time.Minute
	defaultMaxConnLifetime   = 30 * time.Minute
	defaultHealthCheckPeriod = 1 * time.Minute
)

// PoolOption is a function that configures a [puddle.Client].
type PoolOption func(config *Client)

// WithMaxPoolSize sets the maximum number of connections in the pool.
// By default, is defaultMaxPoolSize.
func WithMaxPoolSize(maxPoolSize int32) PoolOption {
	return func(config *Client) {
		config.maxPoolSize = maxPoolSize
	}
}

// WithMaxConnIdleTime sets the maximum amount of time a connection may be idle.
// By default, is defaultMaxConnIdleTime.
func WithMaxConnIdleTime(maxConnIdleTime time.Duration) PoolOption {
	return func(config *Client) {
		config.maxConnIdleTime = maxConnIdleTime
	}
}

// WithMaxConnLifetime sets the maximum amount of time a connection may be reused.
// By default, is defaultMaxConnLifetime.
func WithMaxConnLifetime(maxConnLifetime time.Duration) PoolOption {
	return func(config *Client) {
		config.maxConnLifetime = maxConnLifetime
	}
}

// WithHealthCheckPeriod sets the period between health checks.
// By default, is defaultHealthCheckPeriod.
func WithHealthCheckPeriod(healthCheckPeriod time.Duration) PoolOption {
	return func(config *Client) {
		config.healthCheckPeriod = healthCheckPeriod
	}
}

// Client represents the FTP server storage credential.
type Client struct {
	Host              string `json:"host"`
	Port              int    `json:"port"`
	Username          string `json:"username"`
	Password          string `json:"password"`
	maxPoolSize       int32
	maxConnIdleTime   time.Duration
	maxConnLifetime   time.Duration
	healthCheckPeriod time.Duration
}

// NewClient creates a new FTP client.
func NewClient(
	host string,
	port int,
	username, password string,
	options ...PoolOption,
) (c *Client) {
	c = &Client{
		Host:              host,
		Port:              port,
		Username:          username,
		Password:          password,
		maxPoolSize:       defaultMaxPoolSize,
		maxConnIdleTime:   defaultMaxConnIdleTime,
		maxConnLifetime:   defaultMaxConnLifetime,
		healthCheckPeriod: defaultHealthCheckPeriod,
	}
	for _, opt := range options {
		opt(c)
	}
	return
}

func (c Client) GetConnectionPool(logger logr.Logger) protoc.ConnectionPool {
	return newConnectionPool(logger, c.maxPoolSize, c.maxConnIdleTime, c.maxConnLifetime, c.healthCheckPeriod)
}

func (c Client) GetCredential() any {
	return c
}

func (c Client) GetConnectionID() string {
	return uuid.NewSHA1(
		connectionIDNamespace,
		[]byte(fmt.Sprintf("%s:%d:%s:%s", c.Host, c.Port, c.Username, c.Password)),
	).String()
}

// GetAddress returns the address of the FTP server.
func (c Client) GetAddress() (addr string) {
	return protocutils.BuildAddress(c.Host, c.Port)
}

func (c Client) GetS3API() protoc.S3API {
	panic(errors.ErrUnsupported)
}
