package s3

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/derektruong/fxfer/internal/fileutils"
	"github.com/derektruong/fxfer/internal/iometer"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/s3"
	"github.com/derektruong/fxfer/storage"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const meterNamePrefix = "transfer/storage/s3"

type s3Client struct {
	bucket string
	client protoc.S3API
}

type Source struct {
	logger logr.Logger

	connsMu sync.Mutex
	conns   map[string]*s3Client

	// bytesTransferredMu and bytesTransferred are used to calculate the total
	// bytes downloaded
	bytesTransferredMu sync.RWMutex
	bytesTransferred   map[string]*int64

	// totalHosts is used to store the total number of s3 hosts connected
	totalHosts int32
}

func NewSource(logger logr.Logger) (s *Source) {
	s = &Source{
		logger:           logger.WithName("s3.source"),
		conns:            make(map[string]*s3Client),
		bytesTransferred: make(map[string]*int64),
	}
	return
}

func (s *Source) Close() {
	atomic.AddInt32(&s.totalHosts, -int32(len(s.conns)))
	s.logger.Info("closed s3 source")
}

func (s *Source) GetFileInfo(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (info xferfile.Info, err error) {
	var conn *s3Client
	if conn, err = s.checkAndSetClient(cli); err != nil {
		return
	}
	var objInfo *awss3.HeadObjectOutput
	if objInfo, err = conn.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(conn.bucket),
		Key:    aws.String(filePath),
	}); err != nil {
		return
	}
	var fileName, fileExt string
	if _, fileName, fileExt, err = fileutils.ExtractFileParts(filePath); err != nil {
		return
	}
	info = xferfile.Info{
		Path:      filePath,
		Size:      lo.FromPtr(objInfo.ContentLength),
		Name:      fileName,
		Extension: fileExt,
		ModTime:   lo.FromPtr(objInfo.LastModified),
	}
	return
}

func (s *Source) GetFileFromOffset(
	ctx context.Context,
	filePath string,
	offset int64,
	cli protoc.Client,
) (reader io.ReadCloser, err error) {
	var conn *s3Client
	if conn, err = s.checkAndSetClient(cli); err != nil {
		return
	}
	offsetStr := strconv.FormatInt(offset, 10)
	var objOutput *awss3.GetObjectOutput
	if objOutput, err = conn.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(conn.bucket),
		Key:    aws.String(filePath),
		Range:  aws.String(fmt.Sprintf("bytes=%s-", offsetStr)),
	}); err != nil {
		return
	}
	connID := cli.GetConnectionID()
	transferred := s.getBytesTransferred(connID)
	if transferred == nil {
		s.setBytesTransferred(connID)
		transferred = s.getBytesTransferred(connID)
	}
	reader = iometer.NewTransferReader(objOutput.Body, transferred)
	return
}

func (s *Source) checkAndSetClient(protocol protoc.Client) (conn *s3Client, err error) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	connID := protocol.GetConnectionID()
	var exists bool
	if conn, exists = s.conns[connID]; !exists {
		// setup connection
		client := protocol.GetS3API()
		cred, ok := protocol.GetCredential().(s3.Client)
		if !ok {
			err = storage.ErrS3ProtocolClientInvalid
			return
		}
		conn = &s3Client{
			bucket: cred.BucketName,
			client: client,
		}
		s.conns[connID] = conn
	}
	return
}

func (s *Source) setupMetricMeter(connID string, credential s3.Client) (err error) {
	// setup meter
	meter := otel.GetMeterProvider().Meter(
		fmt.Sprintf("%s/source/%s", meterNamePrefix, credential.GetURI()),
	)

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
}
