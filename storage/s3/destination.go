// Package s3 provides a storage backend using AWS S3 or compatible servers.
//
// In order to allow this backend to function properly, the user accessing the
// bucket must have at least following AWS IAM policy permissions for the
// bucket and all of its sub resources:
//
//	s3:AbortMultipartUpload
//	s3:DeleteObject
//	s3:GetObject
//	s3:ListMultipartUploadParts
//	s3:PutObject
//
// While this package uses the official AWS SDK for Go, Destination is able
// to work with any S3-compatible service such as MinIO. In order to change
// the HTTP endpoint used for sending requests to, adjust the `BaseEndpoint`
// option in the AWS SDK For Go V2 (https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3#Options).
//
// # Implementation
//
// Once a new transfer is initiated, multiple objects in S3 are created:
//
// First of all, a new info object is stored which contains a JSON-encoded blob
// of general information about the upload including its size and metadata.
// This kind of object have the suffix ".info" in their key.
//
// In addition, a new multipart upload
// (http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html) is
// created.
//
// If metadata is associated with the upload during creation, it will be added
// to the multipart upload and after finishing it, the metadata will be passed
// to the final object. However, the metadata which will be attached to the
// final object can only contain ASCII characters and every non-ASCII character
// will be replaced by a question mark (for examples, "Menü" will be "Men?").
// However, this does not apply for the metadata returned by the GetInfo
// function since it relies on the info object for reading the metadata.
// Therefore, HEAD responses will always contain the unchanged metadata, Base64-
// encoded, even if it contains non-ASCII characters.
//
// Once the upload is finished, the multipart upload is completed, resulting in
// the entire file being stored in the bucket. The info object, containing
// metadata is not deleted. It is recommended to copy the finished upload to
// another bucket to avoid it being deleted by the Termination extension.
//
// If an upload is about to being terminated, the multipart upload is aborted
// which removes all the uploaded parts from the bucket. In addition, the
// info object is also deleted. If the upload has been finished already, the
// finished object containing the entire upload is also removed.
//
// # Considerations
//
// In order to support transfer' principle of resumable upload, S3's Multipart-Uploads
// are internally used.
//
// When receiving a PATCH request, its body will be temporarily stored on disk.
// This requirement has been made to ensure the minimum size of a single part
// and to allow the AWS SDK to calculate a checksum. Once the part has been uploaded
// to S3, the temporary file will be removed immediately. Therefore, please
// ensure that the server running this storage backend has enough disk space
// available to hold these caches.
//
// In addition, it must be mentioned that AWS S3 only offers eventual
// consistency (https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel).
// Therefore, it is required to build additional measurements in order to
// prevent concurrent access to the same upload resources which may result in
// data corruption.
package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go"
	"github.com/derektruong/fxfer/internal/fileutils"
	"github.com/derektruong/fxfer/internal/iometer"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/s3"
	"github.com/derektruong/fxfer/storage"
	"github.com/go-logr/logr"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"golang.org/x/exp/slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	bucketMeta       = "bucket"
	objectKeyMeta    = "objectKey"
	multipartKeyMeta = "multipartKey"
	multipartIDMeta  = "multipartID"
	isSinglePartMeta = "isSinglePart"
)

type s3Upload struct {
	store *Destination

	// bucket is the S3 bucket to use for the upload
	bucket string

	// client represents the S3 client to use for the upload
	client protoc.S3API

	// objectKey is the object key under which we save the final file
	objectKey string

	// multipartKey is the object key under which we save the multipart upload
	multipartKey string

	// multipartID is the ID given by S3 to us for the multipart upload
	multipartID string

	// info stores the upload's current FileInfo struct. It may be nil if it hasn't
	// been fetched yet from S3. Never read or write to it directly but instead use
	// the GetInfo and writeInfo functions.
	info *xferfile.Info

	// uploadSemaphore limits the number of concurrent multipart part uploads to S3.
	uploadSemaphore *semaphore.Weighted

	// parts collects all parts for this upload. It will be nil if info is nil as well.
	parts []*s3Part

	// incompletePartSize is the size of an incomplete part object, if one exists. It will be 0 if info is nil as well.
	incompletePartSize int64

	// temporaryDirectory is the path where Destination will create temporary files
	temporaryDirectory string
}

// s3Part represents a single part of a S3 multipart upload.
type s3Part struct {
	number int32
	size   int64
	etag   string
}

type Destination struct {
	// MetadataObjectPrefix is prepended to the name of each .info and .part S3
	// object that is created. If it is not set, then ObjectPrefix is used.
	//
	// Note: With customization in the Upload server, we should not use this field, as it will be overwritten by
	// the storage.SetFilePrefix function.
	MetadataObjectPrefix string

	// MaxObjectSize is the maximum size an S3 Object can have according to S3
	// API specifications. See link above.
	MaxObjectSize int64

	// MinPartSize specifies the minimum size of a single part uploaded to S3
	// in bytes. This number needs to match with the underlying S3 backend or else
	// uploaded parts will be rejected. AWS S3, for examples, uses 5MB for this value.
	MinPartSize int64

	// MaxPartSize specifies the maximum size of a single part uploaded to S3
	// in bytes. This value must be bigger than MinPartSize! In order to
	// choose the correct number, two things have to be kept in mind:
	//
	// If this value is too big and uploading the part to S3 is interrupted
	// expectedly, the entire part is discarded and the end user is required
	// to resume the upload and re-upload the entire big part. In addition, the
	// entire part must be written to disk before submitting to S3.
	//
	// If this value is too low, a lot of requests to S3 may be made, depending
	// on how fast data is coming in. This may result in an eventual overhead.
	MaxPartSize int64

	// PreferredPartSize specifies the preferred size of a single part uploaded to
	// S3. Destination will attempt to slice the incoming data into parts with this
	// size whenever possible. In some cases, smaller parts are necessary, so
	// not every part may reach this value. The PreferredPartSize must be inside the
	// range of MinPartSize to MaxPartSize.
	PreferredPartSize int64

	// MaxMultipartParts is the maximum number of parts an S3 multipart upload is
	// allowed to have according to AWS S3 API specifications.
	// See: http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
	MaxMultipartParts int64

	// MaxBufferedParts is the number of additional parts that can be received from
	// the client and stored on disk while a part is being uploaded to S3. This
	// can help improve throughput by not blocking the client while transfer is
	// communicating with the S3 API, which can have unpredictable latency.
	MaxBufferedParts int64

	// TemporaryDirectory is the path where Destination will create temporary files
	// on disk during the upload. An empty string ("", the default value) will
	// cause Destination to use the operating system's default temporary directory.
	TemporaryDirectory string

	// DisableContentHashes instructs the Destination to not calculate the MD5 and SHA256
	// hashes when uploading data to S3. These hashes are used for file integrity checks
	// and for authentication. However, these hashes also consume a significant amount of
	// CPU, so it might be desirable to disable them.
	// Note that this property is experimental and might be removed in the future!
	DisableContentHashes bool

	// logger: An instance of logr.Logger for logging purposes.
	logger logr.Logger

	// connsMu and conns are used to protect the connection pool for s3 connections
	connsMu sync.Mutex
	conns   map[string]*s3Client
}

// NewDestination constructs a new storage using the supplied bucket and service object.
func NewDestination(logger logr.Logger) (d *Destination) {
	d = &Destination{
		MaxObjectSize:      5 * 1024 * 1024 * 1024 * 1024, // 5TB
		MinPartSize:        5 * 1024 * 1024,               // 5MB
		MaxPartSize:        5 * 1024 * 1024 * 1024,        // 5GB
		PreferredPartSize:  50 * 1024 * 1024,              // 50MB
		MaxMultipartParts:  10000,
		MaxBufferedParts:   20,
		TemporaryDirectory: "",
		logger:             logger.WithName("s3.destination"),
		conns:              make(map[string]*s3Client),
	}
	return
}

func (d *Destination) Close() {
	d.logger.Info("closed s3 destination")
}

func (d *Destination) GetFileInfo(
	ctx context.Context,
	filePath string,
	cli protoc.Client,
) (info xferfile.Info, err error) {
	var s3Cli *s3Client
	if s3Cli, err = d.checkAndSetClient(cli); err != nil {
		return
	}

	upload := d.getUpload(filePath, s3Cli.bucket, s3Cli.client)

	// fetch the info object from S3
	if err = upload.setInternalInfo(ctx); err != nil {
		return
	}
	info = *upload.info
	return
}

func (d *Destination) CreateFile(
	ctx context.Context,
	path string, size int64, modTime time.Time,
	cli protoc.Client,
) (err error) {
	if size > d.MaxObjectSize {
		return fmt.Errorf("file size exceeds maximum object size (%d > %d)", size, d.MaxObjectSize)
	}

	var prefix, fileName, fileExt string
	if prefix, fileName, fileExt, err = fileutils.ExtractFileParts(path); err != nil {
		return
	}

	var s3Cli *s3Client
	if s3Cli, err = d.checkAndSetClient(cli); err != nil {
		return
	}

	// prepare transfer file info
	info := xferfile.Info{
		Path:      path,
		Size:      size,
		ModTime:   modTime,
		StartTime: time.Now(),
		Name:      fileName,
		Extension: fileExt,
	}

	res, err := s3Cli.client.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket: aws.String(s3Cli.bucket),
		Key:    &path,
	})
	if err != nil {
		return fmt.Errorf("unable to create multipart upload: %w", err)
	}

	// store the multipart upload ID in the metadata
	info.Metadata = map[string]string{
		bucketMeta:       s3Cli.bucket,
		objectKeyMeta:    path,
		multipartKeyMeta: filepath.Join(prefix, fileName+".part"),
		multipartIDMeta:  *res.UploadId,
	}

	// if size < MinPartSize, we can upload the file in a single part
	if size <= d.MinPartSize {
		info.Metadata[isSinglePartMeta] = "true"
	}

	// create the info file
	upload := &s3Upload{
		bucket:             s3Cli.bucket,
		objectKey:          path,
		multipartID:        *res.UploadId,
		client:             s3Cli.client,
		info:               &info,
		parts:              make([]*s3Part, 0),
		temporaryDirectory: d.TemporaryDirectory,
	}
	if err = upload.writeInfo(ctx, info); err != nil {
		return fmt.Errorf("unable to create info file: %w", err)
	}
	return
}

func (d *Destination) TransferFileChunk(
	ctx context.Context,
	filePath string,
	src io.Reader,
	offset int64,
	cli protoc.Client,
) (n int64, err error) {
	var s3Cli *s3Client
	if s3Cli, err = d.checkAndSetClient(cli); err != nil {
		return
	}

	// get the upload object
	upload := d.getUpload(filePath, s3Cli.bucket, s3Cli.client)

	// set the info upload if it is not set yet
	if err = upload.setInternalInfo(ctx); err != nil {
		return
	}
	incompletePartSize := upload.incompletePartSize

	// create a transfer reader with rate limiting
	transferReader := iometer.NewTransferReader(src, &offset)
	transferReader.SetRateLimit(upload.calcOptimalSpeed())
	src = io.Reader(transferReader)

	// get the total size of the current upload, number of parts to generate next number and whether
	// an incomplete part exists
	if incompletePartSize > 0 {
		var incompletePartFile *os.File
		if incompletePartFile, err = upload.downloadIncompletePartForUpload(ctx); err != nil {
			return 0, err
		}
		if incompletePartFile == nil {
			return 0, fmt.Errorf("expected an incomplete part file but did not get any")
		}
		defer cleanUpTempFile(incompletePartFile)

		if err = upload.deleteIncompletePartForUpload(ctx); err != nil {
			return 0, err
		}

		// prepend an incomplete part, if necessary and adapt the offset
		src = io.MultiReader(incompletePartFile, src)
		offset = offset - incompletePartSize
	}

	bytesUploaded, err := upload.uploadParts(ctx, offset, src)

	// the size of the incomplete part should not be counted, because the
	// process of the incomplete part should be fully transparent to the user.
	bytesUploaded = max(bytesUploaded - incompletePartSize, 0)

	upload.info.Offset += bytesUploaded
	if upload.info.Size == 0 {
		upload.info.Size = bytesUploaded
	}
	return bytesUploaded, err
}

func (d *Destination) FinalizeTransfer(ctx context.Context, filePath string, protocol protoc.Client) (err error) {
	var s3Cli *s3Client
	if s3Cli, err = d.checkAndSetClient(protocol); err != nil {
		return
	}

	upload := d.getUpload(filePath, s3Cli.bucket, s3Cli.client)

	// set the info upload if it is not set yet
	if err = upload.setInternalInfo(ctx); err != nil {
		return
	}
	parts := upload.parts

	if len(parts) == 0 {
		// AWS expects at least one part to be present when completing the multipart.
		// So if the transfer has a size of 0, we create an empty part
		// and use that for completing the multipart.
		var res *awss3.UploadPartOutput
		if res, err = upload.client.UploadPart(ctx, &awss3.UploadPartInput{
			Bucket:     aws.String(upload.bucket),
			Key:        aws.String(upload.objectKey),
			UploadId:   aws.String(upload.multipartID),
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader([]byte{}),
		}); err != nil {
			return
		}
		parts = []*s3Part{
			{
				etag:   *res.ETag,
				number: 1,
				size:   0,
			},
		}
	}

	totalPartSize := int64(0)
	completedParts := lo.Map(parts, func(p *s3Part, _ int) types.CompletedPart {
		totalPartSize += p.size
		return types.CompletedPart{
			ETag:       aws.String(p.etag),
			PartNumber: aws.Int32(p.number),
		}
	})

	if totalPartSize != upload.info.Size {
		return storage.ErrFileOrObjectCannotFinalize
	}

	if _, err = upload.client.CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:   aws.String(upload.bucket),
		Key:      aws.String(upload.objectKey),
		UploadId: aws.String(upload.multipartID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}); err != nil {
		return
	}

	upload.info.Offset = upload.info.Size
	upload.info.FinishTime = time.Now()
	return upload.writeInfo(ctx, *upload.info)
}

func (d *Destination) DeleteFile(ctx context.Context, filePath string, protocol protoc.Client) (err error) {
	var s3Cli *s3Client
	if s3Cli, err = d.checkAndSetClient(protocol); err != nil {
		return
	}

	upload := d.getUpload(filePath, s3Cli.bucket, s3Cli.client)

	// set the info upload if it is not set yet
	if err = upload.setInternalInfo(ctx); err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 0, 3)

	go func() {
		defer wg.Done()

		// abort the multipart upload
		if _, err = upload.client.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
			Bucket:   aws.String(s3Cli.bucket),
			Key:      &filePath,
			UploadId: aws.String(upload.multipartID),
		}); err != nil && !isAwsError[*types.NoSuchUpload](err) {
			errs = append(errs, err)
		}
	}()

	go func() {
		defer wg.Done()

		var infoPath string
		if infoPath, err = xferfile.GenerateInfoPath(filePath); err != nil {
			return
		}

		// delete the info and content files
		var res *awss3.DeleteObjectsOutput
		if res, err = upload.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(s3Cli.bucket),
			Delete: &types.Delete{
				Objects: []types.ObjectIdentifier{
					{
						Key: &filePath,
					},
					{
						Key: lo.ToPtr(upload.info.Metadata[multipartKeyMeta]),
					},
					{
						Key: &infoPath,
					},
				},
				Quiet: aws.Bool(true),
			},
		}); err != nil {
			errs = append(errs, err)
			return
		}

		for _, s3Err := range res.Errors {
			if *s3Err.Code != "NoSuchKey" {
				errs = append(errs, fmt.Errorf("AWS S3 Error (%s) for object %s: %s", *s3Err.Code, *s3Err.Key, *s3Err.Message))
			}
		}
	}()

	wg.Wait()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (d *Destination) getUpload(
	filePath string,
	bucket string,
	client protoc.S3API,
) (upload *s3Upload) {
	upload = &s3Upload{
		store:              d,
		bucket:             bucket,
		client:             client,
		objectKey:          filePath,
		multipartKey:       fmt.Sprintf("%s.part", strings.TrimSuffix(filePath, filepath.Ext(filePath))),
		parts:              make([]*s3Part, 0),
		temporaryDirectory: d.TemporaryDirectory,
		uploadSemaphore:    semaphore.NewWeighted(10),
	}
	return
}

func (d *Destination) calcOptimalPartSize(size int64) (optimalPartSize int64, err error) {
	switch {
	// When upload is smaller or equal to PreferredPartSize, we upload in just one part.
	case size <= d.PreferredPartSize:
		optimalPartSize = d.PreferredPartSize
	// Does the upload fit in MaxMultipartParts parts or less with PreferredPartSize.
	case size <= d.PreferredPartSize*d.MaxMultipartParts:
		optimalPartSize = d.PreferredPartSize
	// Prerequisite: Be aware, that the result of an integer division (x/y) is
	// ALWAYS rounded DOWN, as there are no digits behind the comma.
	// In order to find out, whether we have an exact result or a rounded down
	// one, we can check, whether the remainder of that division is 0 (x%y == 0).
	//
	// So if the result of (size/MaxMultipartParts) is not a rounded down value,
	// then we can use it as our optimalPartSize. But if this division produces a
	// remainder, we have to round up the result by adding +1. Otherwise, our
	// upload would not fit into MaxMultipartParts number of parts with that
	// size. We would need an additional part in order to upload everything.
	// While in almost all cases, we could skip the check for the remainder and
	// just add +1 to every result, but there is one case, where doing that would
	// doom our upload. When (MaxObjectSize == MaxPartSize * MaxMultipartParts),
	// by adding +1, we would end up with an optimalPartSize > MaxPartSize.
	// With the current S3 API specifications, we will not run into this problem,
	// but these specs are subject to change, and there are other stores as well,
	// which are implementing the S3 API (e.g. RIAK, Ceph RadosGW), but might
	// have different settings.
	case size%d.MaxMultipartParts == 0:
		optimalPartSize = size / d.MaxMultipartParts
	// Having a remainder larger than 0 means, the float result would have
	// digits after the comma (e.g. be something like 10.9). As a result, we can
	// only squeeze our upload into MaxMultipartParts parts, if we rounded UP
	// this division's result. That is what is happening here. We round up by
	// adding +1, if the prior test for (remainder == 0) did not succeed.
	default:
		optimalPartSize = size/d.MaxMultipartParts + 1
	}

	// optimalPartSize must never exceed MaxPartSize
	if optimalPartSize > d.MaxPartSize {
		return optimalPartSize, fmt.Errorf("calcOptimalPartSize: to upload %v bytes optimalPartSize %v must exceed MaxPartSize %v", size, optimalPartSize, d.MaxPartSize)
	}
	return optimalPartSize, nil
}

func (d *Destination) checkAndSetClient(protocol protoc.Client) (conn *s3Client, err error) {
	d.connsMu.Lock()
	defer d.connsMu.Unlock()
	connID := protocol.GetConnectionID()
	var exists bool
	if conn, exists = d.conns[connID]; !exists {
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
		d.conns[connID] = conn
	}
	return
}

func (u *s3Upload) writeInfo(ctx context.Context, info xferfile.Info) (err error) {
	var jsonInfo []byte
	if jsonInfo, err = json.Marshal(info); err != nil {
		return
	}
	// create object on S3 containing information about the file
	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(info.Path); err != nil {
		return
	}
	_, err = u.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:        aws.String(u.bucket),
		Key:           &infoPath,
		Body:          bytes.NewReader(jsonInfo),
		ContentLength: aws.Int64(int64(len(jsonInfo))),
	})
	return
}

func (u *s3Upload) uploadParts(ctx context.Context, offset int64, src io.Reader) (int64, error) {
	store := u.store
	parts := u.parts

	size := u.info.Size
	bytesUploaded := int64(0)
	optimalPartSize, err := store.calcOptimalPartSize(size)
	if err != nil {
		return 0, err
	}

	numParts := len(parts)
	nextPartNum := int32(numParts + 1)

	partProducer, fileChan := newS3PartProducer(src, store.MaxBufferedParts, store.TemporaryDirectory)

	producerCtx, cancelProducer := context.WithCancel(ctx)
	defer func() {
		cancelProducer()
		partProducer.closeUnreadFiles()
	}()
	go partProducer.produce(producerCtx, optimalPartSize)

	var eg errgroup.Group

	for {
		// we acquire the semaphore before starting the goroutine to avoid
		// starting many goroutines, most of which are just waiting for the lock.
		// We also acquire the semaphore before reading from the channel to reduce
		// the number of part files are lay-ing around on disk without being used.
		if err = u.acquireUploadSemaphore(ctx); err != nil {
			return 0, err
		}
		chunk, more := <-fileChan
		if !more {
			u.releaseUploadSemaphore()
			break
		}

		partFile := chunk.reader
		partSize := chunk.size
		closePart := chunk.closeReader
		isSinglePart := u.info.Metadata[isSinglePartMeta] == "true"
		isFinalChunk := size == offset+bytesUploaded+partSize

		if partSize >= store.MinPartSize || isFinalChunk || isSinglePart {
			part := &s3Part{
				etag:   "",
				size:   partSize,
				number: nextPartNum,
			}
			parts = append(parts, part)

			eg.Go(func() (err error) {
				defer u.releaseUploadSemaphore()

				uploadPartInput := &awss3.UploadPartInput{
					Bucket:     aws.String(u.bucket),
					Key:        &u.objectKey,
					UploadId:   aws.String(u.multipartID),
					PartNumber: aws.Int32(part.number),
				}
				etag, err := u.putPartForUpload(ctx, uploadPartInput, partFile, part.size)
				if err == nil {
					part.etag = etag
				}

				closeErr := closePart()
				if err != nil {
					return err
				}
				if closeErr != nil {
					return closeErr
				}
				return nil
			})
		} else {
			eg.Go(func() (err error) {
				defer u.releaseUploadSemaphore()

				if err = u.putIncompletePartForUpload(ctx, partFile); err == nil {
					u.incompletePartSize = partSize
				}

				closeErr := closePart()
				if err != nil {
					return err
				}
				if closeErr != nil {
					return closeErr
				}
				return nil
			})
		}

		bytesUploaded += partSize
		nextPartNum++
	}

	if uploadErr := eg.Wait(); uploadErr != nil {
		return 0, uploadErr
	}

	return bytesUploaded, partProducer.err
}

func (u *s3Upload) calcOptimalSpeed() float64 {
	const (
		minSpeed = 1 * 1024 * 1024   // 1 MB/s
		maxSpeed = 150 * 1024 * 1024 // 150 MB/s
		baseTime = 60                // aim for uploads to take at least this many seconds
	)

	objectSize := float64(u.info.Size)

	// calculate base speed
	baseSpeed := objectSize / baseTime

	// apply logarithmic scaling
	scaleFactor := math.Log10(objectSize/1024/1024 + 1) // +1 to avoid log(0)
	scaledSpeed := baseSpeed * scaleFactor

	// apply some randomness (±10%)
	randomFactor := 0.9 + 0.2*rand.Float64()
	finalSpeed := scaledSpeed * randomFactor

	// ensure speed is within bounds
	finalSpeed = math.Max(minSpeed, math.Min(maxSpeed, finalSpeed))

	return finalSpeed
}

func (u *s3Upload) acquireUploadSemaphore(ctx context.Context) error {
	return u.uploadSemaphore.Acquire(ctx, 1)
}

func (u *s3Upload) releaseUploadSemaphore() {
	u.uploadSemaphore.Release(1)
}

func cleanUpTempFile(file *os.File) {
	_ = file.Close()
	_ = os.Remove(file.Name())
}

func (u *s3Upload) putPartForUpload(
	ctx context.Context,
	uploadPartInput *awss3.UploadPartInput, file io.ReadSeeker, size int64,
) (string, error) {
	store := u.store
	if !store.DisableContentHashes {
		// By default, use the traditional approach to upload data
		uploadPartInput.Body = file
		res, err := u.client.UploadPart(ctx, uploadPartInput)
		if err != nil {
			return "", err
		}
		return *res.ETag, nil
	} else {
		// experimental feature to prevent the AWS SDK from calculating the SHA256 hash
		// for the parts we u to S3.
		// We compute the pre-signed URL without the body attached and then send the request
		// on our own. This way, the body is not included in the SHA256 calculation.
		client, ok := u.client.(*awss3.Client)
		if !ok {
			return "", fmt.Errorf("s3store: failed to cast S3 service for presigning")
		}
		presignedClient := awss3.NewPresignClient(client)
		s3Req, err := presignedClient.PresignUploadPart(ctx, uploadPartInput, func(opts *awss3.PresignOptions) {
			opts.Expires = 15 * time.Minute
		})
		if err != nil {
			return "", fmt.Errorf("failed to presign UploadPart: %s", err)
		}
		req, err := http.NewRequest("PUT", s3Req.URL, file)
		if err != nil {
			return "", err
		}

		// set the Content-Length manually to prevent the usage of Transfer-Encoding: chunked,
		// which is not supported by AWS S3.
		req.ContentLength = size

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer res.Body.Close()

		if res.StatusCode != 200 {
			buf := new(strings.Builder)
			_, _ = io.Copy(buf, res.Body)
			return "", fmt.Errorf("unexpected response code %d for presigned u: %s", res.StatusCode, buf.String())
		}

		return res.Header.Get("ETag"), nil
	}
}

func (u *s3Upload) setInternalInfo(ctx context.Context) (err error) {
	if u.info != nil {
		return
	}

	var info xferfile.Info
	var parts []*s3Part
	var incompletePartSize int64

	var infoPath string
	if infoPath, err = xferfile.GenerateInfoPath(u.objectKey); err != nil {
		return
	}

	var wg sync.WaitGroup

	// we store all errors in here and handle them all together once the wait
	// group is done.
	var infoErr error
	var partsErr error
	var incompletePartSizeErr error

	uploadInfoSetFn := func() {
		u.info = &info
		u.parts = parts
		u.incompletePartSize = incompletePartSize
	}

	if u.multipartID != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// get file info stored in separate object
			var res *awss3.GetObjectOutput
			res, infoErr = u.client.GetObject(ctx, &awss3.GetObjectInput{
				Bucket: aws.String(u.bucket),
				Key:    &infoPath,
			})
			if infoErr == nil {
				infoErr = json.NewDecoder(res.Body).Decode(&info)
			}
		}()
	} else {
		// if the multipartID is empty, we need to fetch it from the info object before
		// the following goroutines are started get file info stored in separate object
		var res *awss3.GetObjectOutput
		res, infoErr = u.client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(u.bucket),
			Key:    &infoPath,
		})
		if infoErr == nil {
			infoErr = json.NewDecoder(res.Body).Decode(&info)
		}
	}
	wg.Wait()

	wg.Add(2)
	go func() {
		defer wg.Done()
		if u.multipartID == "" && info.Metadata[multipartIDMeta] != "" {
			u.multipartID = info.Metadata[multipartIDMeta]
		}
		// get uploaded parts and their offset
		parts, partsErr = u.listAllParts(ctx)
	}()

	go func() {
		defer wg.Done()
		// get size of optional incomplete part file.
		incompletePartSize, incompletePartSizeErr = u.headIncompletePartForUpload(ctx)
	}()
	wg.Wait()

	// finally, after all requests are complete, let's handle the errors
	if infoErr != nil {
		// if the info file is not found, we consider the upload to be non-existent
		if err = infoErr; isAwsError[*types.NoSuchKey](err) {
			err = xferfile.ErrFileNotExists
		}
		return
	}

	if partsErr != nil {
		err = partsErr
		// check if the error is caused by the multipart upload not being found. This happens
		// when the multipart upload has already been completed or aborted. Since
		// we already found the info object, we know that the upload has been
		// completed and therefore can ensure the offset is the size.
		// AWS S3 returns NoSuchUpload, but other implementations, such as DigitalOcean
		// Spaces, can also return NoSuchKey.

		// The AWS Go SDK v2 has a bug where types.NoSuchUpload is not returned,
		// so we also need to check the error code itself.
		// See https://github.com/aws/aws-sdk-go-v2/issues/1635
		// In addition, S3-compatible storages, like DigitalOcean Spaces, might cause
		// types.NoSuchKey to not be returned as well.
		if isAwsError[*types.NoSuchUpload](err) || isAwsErrorCode(err, "NoSuchUpload") ||
			isAwsError[*types.NoSuchKey](err) || isAwsErrorCode(err, "NoSuchKey") {
			info.Offset = info.Size
			uploadInfoSetFn()
			err = nil
		}
		return
	}

	if incompletePartSizeErr != nil {
		err = incompletePartSizeErr
		return
	}

	// the offset is the sum of all part sizes and the size of the incomplete part file.
	offset := incompletePartSize
	for _, part := range parts {
		offset += part.size
	}

	// if the offset is larger than the size, we set the offset to the size
	info.Offset = offset
	// set upload object
	uploadInfoSetFn()
	return nil
}

func (u *s3Upload) listAllParts(ctx context.Context) (parts []*s3Part, err error) {
	var partMarker *string
	for {
		// get uploaded parts
		var listPart *awss3.ListPartsOutput
		if listPart, err = u.client.ListParts(ctx, &awss3.ListPartsInput{
			Bucket:           aws.String(u.bucket),
			Key:              aws.String(u.objectKey),
			UploadId:         aws.String(u.multipartID),
			PartNumberMarker: partMarker,
		}); err != nil {
			return
		}
		parts = slices.Grow(parts, len(parts)+len((*listPart).Parts))
		for _, part := range (*listPart).Parts {
			parts = append(parts, &s3Part{
				number: *part.PartNumber,
				size:   *part.Size,
				etag:   *part.ETag,
			})
		}
		if listPart.IsTruncated != nil && *listPart.IsTruncated {
			partMarker = listPart.NextPartNumberMarker
		} else {
			break
		}
	}
	return parts, nil
}

func (u *s3Upload) downloadIncompletePartForUpload(ctx context.Context) (*os.File, error) {
	incompleteUploadObject, err := u.getIncompletePartForUpload(ctx)
	if err != nil {
		return nil, err
	}
	if incompleteUploadObject == nil {
		// We did not find an incomplete upload
		return nil, nil
	}
	defer incompleteUploadObject.Body.Close()

	partFile, err := os.CreateTemp(u.temporaryDirectory, "file-transfer-s3-tmp-")
	if err != nil {
		return nil, err
	}

	n, err := io.Copy(partFile, incompleteUploadObject.Body)
	if err != nil {
		return nil, err
	}
	if n < *incompleteUploadObject.ContentLength {
		return nil, errors.New("short read of incomplete upload")
	}

	_, err = partFile.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return partFile, nil
}

func (u *s3Upload) getIncompletePartForUpload(ctx context.Context) (*awss3.GetObjectOutput, error) {
	obj, err := u.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    lo.ToPtr(u.multipartKey),
	})
	if err != nil && (isAwsError[*types.NoSuchKey](err) || isAwsError[*types.NotFound](err) || isAwsErrorCode(err, "AccessDenied")) || isAwsErrorCode(err, "Forbidden") {
		return nil, nil
	}
	return obj, err
}

func (u *s3Upload) headIncompletePartForUpload(ctx context.Context) (int64, error) {
	obj, err := u.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    lo.ToPtr(u.multipartKey),
	})

	if err != nil {
		if isAwsError[*types.NoSuchKey](err) || isAwsError[*types.NotFound](err) || isAwsErrorCode(err, "AccessDenied") || isAwsErrorCode(err, "Forbidden") {
			err = nil
		}
		return 0, err
	}

	return *obj.ContentLength, nil
}

func (u *s3Upload) putIncompletePartForUpload(ctx context.Context, file io.ReadSeeker) error {
	_, err := u.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    lo.ToPtr(u.multipartKey),
		Body:   file,
	})
	return err
}

func (u *s3Upload) deleteIncompletePartForUpload(ctx context.Context) (err error) {
	_, err = u.client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    lo.ToPtr(u.multipartKey),
	})
	return
}

// isAwsError tests whether an error object is an instance of the AWS error
// specified by its code.
func isAwsError[T error](err error) bool {
	var awsErr T
	return errors.As(err, &awsErr)
}

func isAwsErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == code
	}
	return false
}
