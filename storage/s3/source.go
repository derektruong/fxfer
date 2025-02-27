package s3

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/derektruong/fxfer/internal/fileutils"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/s3"
	"github.com/derektruong/fxfer/storage"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
)

type s3Client struct {
	bucket string
	client protoc.S3API
}

type Source struct {
	logger logr.Logger

	connsMu sync.Mutex
	conns   map[string]*s3Client
}

func NewSource(logger logr.Logger) (s *Source) {
	s = &Source{
		logger: logger.WithName("s3.source"),
		conns:  make(map[string]*s3Client),
	}
	return
}

func (s *Source) Close() {
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
	reader = objOutput.Body
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
