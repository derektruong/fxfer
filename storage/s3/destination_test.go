package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/internal/xferfile/xferfiletest"
	mock_protoc "github.com/derektruong/fxfer/protoc/mock"
	s3_protoc "github.com/derektruong/fxfer/protoc/s3"
	"github.com/derektruong/fxfer/storage"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Destination", func() {
	var (
		err            error
		mockCtrl       *gomock.Controller
		mockS3API      *mock_protoc.MockS3API
		mockClient     *mock_protoc.MockClient
		destStorage    *Destination
		s3ProtocClient *s3_protoc.Client
		fileInfo       xferfile.Info
		infoPath       string
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		DeferCleanup(mockCtrl.Finish)
		mockS3API = mock_protoc.NewMockS3API(mockCtrl)
		mockClient = mock_protoc.NewMockClient(mockCtrl)
		destStorage = destStorageFactory(nil)
		s3ProtocClient = s3_protoc.NewClient(endpoint, bucketName, region, accessKey, secretKey)
		fileInfo = xferfiletest.InfoFactory(nil)

		infoPath, err = xferfile.GenerateInfoPath(fileInfo.Path)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("GetFileInfo", func() {
		It("should return file info successfully", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						PartNumber: aws.Int32(1),
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-1"),
					},
					{
						PartNumber: aws.Int32(2),
						Size:       aws.Int64(200),
						ETag:       aws.String("etag-2"),
					},
				},
				NextPartNumberMarker: aws.String("2"),
				// Simulate a truncated response, so s3store should send a second request
				IsTruncated: aws.Bool(true),
			}, nil)
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: aws.String("2"),
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						PartNumber: aws.Int32(3),
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-3"),
					},
				},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, gomock.Any()).
				Do(func(ctx context.Context, input *awss3.HeadObjectInput, opts ...func(*awss3.Options)) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Metadata[multipartKeyMeta]))
				}).
				Return(nil, &types.NoSuchKey{})

			info, err := destStorage.GetFileInfo(ctx, fileInfo.Path, mockClient)
			Expect(err).ToNot(HaveOccurred())
			Expect(info).To(And(
				HaveField("Path", fileInfo.Path),
				HaveField("Size", fileInfo.Size),
				HaveField("Name", fileInfo.Name),
				HaveField("Extension", fileInfo.Extension),
				HaveField("ModTime", BeTemporally("~", fileInfo.ModTime, 2*time.Second)),
				HaveField("StartTime", BeTemporally("~", fileInfo.StartTime, 2*time.Second)),
				HaveField("FinishTime", BeTemporally("~", fileInfo.FinishTime, 2*time.Second)),
				HaveField("Offset", BeNumerically("==", 400)),
				HaveField("Checksum", fileInfo.Checksum),
				HaveField("Metadata", And(
					HaveKeyWithValue("bucket", bucketName),
					HaveKeyWithValue("multipartID", "test-multipart-id"),
					HaveKeyWithValue("multipartKey", fileInfo.Metadata[multipartKeyMeta]),
					HaveKeyWithValue("objectKey", fileInfo.Path),
				)),
			))
		}, NodeTimeout(10*time.Second))

		It("should return error when info file not found", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				Return(nil, &types.NoSuchKey{
					Message: aws.String("info file not found"),
				})
			mockS3API.EXPECT().ListParts(ctx, gomock.Any()).Return(&awss3.ListPartsOutput{}, nil)
			mockS3API.EXPECT().HeadObject(ctx, gomock.Any()).Return(&awss3.HeadObjectOutput{
				ContentLength: &fileInfo.Size,
			}, nil)

			_, err := destStorage.GetFileInfo(ctx, fileInfo.Path, mockClient)
			Expect(err).To(MatchError(xferfile.ErrFileNotExists))
		}, NodeTimeout(10*time.Second))

		It("should return the correct file info when uploading with incomplete part", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{Parts: []types.Part{}}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(&awss3.HeadObjectOutput{
				ContentLength: aws.Int64(10),
			}, nil)

			info, err := destStorage.GetFileInfo(ctx, fileInfo.Path, mockClient)
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Offset).To(Equal(int64(10)))
		}, NodeTimeout(10*time.Second))

		It("should return the correct file info when uploading finished", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(nil, &types.NoSuchUpload{})
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &types.NoSuchKey{})

			info, err := destStorage.GetFileInfo(ctx, fileInfo.Path, mockClient)
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Size).To(Equal(fileInfo.Size))
			Expect(info.Offset).To(Equal(fileInfo.Size))
		}, NodeTimeout(10*time.Second))
	})

	Describe("CreateFile", func() {
		It("should create new file successfully", func(ctx context.Context) {
			gomock.InOrder(
				mockClient.EXPECT().GetConnectionID().
					Return(uuid.NewString()),
				mockClient.EXPECT().GetS3API().Return(mockS3API),
				mockClient.EXPECT().GetCredential().
					Return(*s3ProtocClient),
				mockS3API.EXPECT().CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(fileInfo.Path),
				}).Return(&awss3.CreateMultipartUploadOutput{
					UploadId: aws.String("test-multipart-id"),
				}, nil),
				mockS3API.EXPECT().PutObject(ctx, gomock.Any()).
					DoAndReturn(func(
						ctx context.Context,
						input *awss3.PutObjectInput,
						opts ...func(*awss3.Options),
					) (*awss3.PutObjectOutput, error) {
						Expect(*input.Bucket).To(Equal(bucketName))
						Expect(*input.Key).To(Equal(infoPath))
						Expect(input.Body).ToNot(BeNil())
						Expect(*input.ContentLength).To(BeNumerically(">", 0))
						var gotInfo xferfile.Info
						Expect(json.NewDecoder(input.Body).Decode(&gotInfo)).To(Succeed())
						Expect(gotInfo.Path).To(Equal(fileInfo.Path))
						Expect(gotInfo.Name).To(Equal(fileInfo.Name))
						Expect(gotInfo.Extension).To(Equal(fileInfo.Extension))
						Expect(gotInfo.Size).To(Equal(fileInfo.Size))
						Expect(gotInfo.ModTime).To(BeTemporally("~", fileInfo.ModTime, 2*time.Second))
						Expect(gotInfo.StartTime).
							To(BeTemporally("~", time.Now(), 2*time.Second))
						Expect(gotInfo.Offset).To(Equal(int64(0)))
						Expect(gotInfo.Metadata).To(And(
							HaveKeyWithValue("bucket", bucketName),
							HaveKeyWithValue("multipartID", "test-multipart-id"),
							HaveKeyWithValue(
								"multipartKey",
								strings.Replace(fileInfo.Path, filepath.Ext(fileInfo.Path), ".part", -1),
							),
							HaveKeyWithValue("objectKey", fileInfo.Path),
						))
						return nil, nil
					}),
			)

			Expect(destStorage.CreateFile(
				ctx,
				fileInfo.Path, fileInfo.Size, fileInfo.ModTime,
				mockClient,
			)).To(Succeed())
		}, NodeTimeout(10*time.Second))

		// This test ensures that a newly created upload without any chunks can be
		// directly finished. There are no calls to ListPart or HeadObject because
		// the upload is not fetched from S3 first.
		It("should create an empty file successfully", func(ctx context.Context) {
			fileInfo.Size = 0
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Path),
			}).Return(&awss3.CreateMultipartUploadOutput{
				UploadId: aws.String("test-multipart-id"),
			}, nil)
			mockS3API.EXPECT().PutObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.PutObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.PutObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))
					Expect(input.Body).ToNot(BeNil())
					Expect(*input.ContentLength).To(BeNumerically(">", 0))
					var gotInfo xferfile.Info
					Expect(json.NewDecoder(input.Body).Decode(&gotInfo)).To(Succeed())
					Expect(gotInfo.Path).To(Equal(fileInfo.Path))
					Expect(gotInfo.Name).To(Equal(fileInfo.Name))
					Expect(gotInfo.Extension).To(Equal(fileInfo.Extension))
					Expect(gotInfo.Size).To(Equal(fileInfo.Size))
					Expect(gotInfo.ModTime).To(BeTemporally("~", fileInfo.ModTime, 2*time.Second))
					Expect(gotInfo.StartTime).
						To(BeTemporally("~", time.Now(), 2*time.Second))
					Expect(gotInfo.Offset).To(Equal(int64(0)))
					Expect(gotInfo.Metadata).To(And(
						HaveKeyWithValue("bucket", bucketName),
						HaveKeyWithValue("multipartID", "test-multipart-id"),
						HaveKeyWithValue(
							"multipartKey",
							strings.Replace(fileInfo.Path, filepath.Ext(fileInfo.Path), ".part", -1),
						),
						HaveKeyWithValue("objectKey", fileInfo.Path),
					))
					return nil, nil
				})
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, gomock.Any()).Return(&awss3.ListPartsOutput{}, nil)
			mockS3API.EXPECT().HeadObject(ctx, gomock.Any()).Return(&awss3.HeadObjectOutput{
				ContentLength: &fileInfo.Size,
			}, nil)
			mockS3API.EXPECT().UploadPart(ctx, gomock.Any()).
				Do(func(ctx context.Context, input *awss3.UploadPartInput, opts ...func(*awss3.Options)) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Path))
					Expect(*input.UploadId).To(Equal("test-multipart-id"))
					Expect(*input.PartNumber).To(Equal(int32(1)))
				}).
				Return(&awss3.UploadPartOutput{
					ETag: aws.String("etag"),
				}, nil)
			mockS3API.EXPECT().CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
				Bucket:   aws.String(bucketName),
				Key:      aws.String(fileInfo.Path),
				UploadId: aws.String("test-multipart-id"),
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{
							ETag:       aws.String("etag"),
							PartNumber: aws.Int32(1),
						},
					},
				},
			}).Return(nil, nil)
			mockS3API.EXPECT().PutObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.PutObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.PutObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))
					Expect(input.Body).ToNot(BeNil())
					Expect(*input.ContentLength).To(BeNumerically(">", 0))
					var gotInfo xferfile.Info
					Expect(json.NewDecoder(input.Body).Decode(&gotInfo)).To(Succeed())
					Expect(gotInfo.Path).To(Equal(fileInfo.Path))
					Expect(gotInfo.Name).To(Equal(fileInfo.Name))
					Expect(gotInfo.Extension).To(Equal(fileInfo.Extension))
					Expect(gotInfo.Size).To(Equal(fileInfo.Size))
					Expect(gotInfo.ModTime).To(BeTemporally("~", fileInfo.ModTime, 2*time.Second))
					Expect(gotInfo.StartTime).NotTo(BeZero())
					Expect(gotInfo.FinishTime).NotTo(BeZero())
					Expect(gotInfo.Offset).To(BeZero())
					Expect(gotInfo.Metadata).To(And(
						HaveKeyWithValue("bucket", bucketName),
						HaveKeyWithValue("multipartID", "test-multipart-id"),
						HaveKeyWithValue(
							"multipartKey",
							strings.Replace(fileInfo.Path, filepath.Ext(fileInfo.Path), ".part", -1),
						),
						HaveKeyWithValue("objectKey", fileInfo.Path),
					))
					return nil, nil
				})

			Expect(destStorage.CreateFile(
				ctx,
				fileInfo.Path, fileInfo.Size, fileInfo.ModTime,
				mockClient,
			)).To(Succeed())
			Expect(destStorage.FinalizeTransfer(ctx, fileInfo.Path, mockClient)).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should return error when creating file larger max object size", func(ctx context.Context) {
			fileInfo.Size = destStorage.MaxObjectSize + 1
			err := destStorage.CreateFile(
				ctx,
				fileInfo.Path, fileInfo.Size, fileInfo.ModTime,
				mockClient,
			)
			Expect(err.Error()).To(ContainSubstring("file size exceeds maximum object size"))
		}, NodeTimeout(10*time.Second))

		It("should return error when creating file with missing file extension", func(ctx context.Context) {
			fileInfo.Path = strings.TrimSuffix(fileInfo.Path, filepath.Ext(fileInfo.Path))
			err := destStorage.CreateFile(
				ctx,
				fileInfo.Path, fileInfo.Size, fileInfo.ModTime,
				mockClient,
			)
			Expect(err.Error()).To(ContainSubstring("file extension is required"))
		}, NodeTimeout(10*time.Second))
	})

	Describe("TransferFileChunk", func() {
		It("should write chunk successfully", func(ctx context.Context) {
			fileInfo.Size = 500
			fileInfo.Size = 0
			destStorage = destStorageFactory(func(s *Destination) {
				s.MaxPartSize = 8
				s.MinPartSize = 4
				s.PreferredPartSize = 4
				s.MaxMultipartParts = 10000
				s.MaxObjectSize = 5 * 1024 * 1024 * 1024 * 1024
			})

			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-1"),
						PartNumber: aws.Int32(1),
					},
					{
						Size:       aws.Int64(200),
						ETag:       aws.String("etag-2"),
						PartNumber: aws.Int32(2),
					},
				},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &types.NoSuchKey{})

			// transfer chunks
			mockS3API.EXPECT().UploadPart(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.UploadPartInput,
					opts ...func(*awss3.Options),
				) (*awss3.UploadPartOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Path))
					Expect(*input.UploadId).To(Equal("test-multipart-id"))

					switch *input.PartNumber {
					case int32(3):
						buf := make([]byte, 4)
						_, err := input.Body.Read(buf)
						Expect(err).ToNot(HaveOccurred())
						Expect(buf).To(Equal([]byte("1234")))
						return &awss3.UploadPartOutput{
							ETag: aws.String("etag-3"),
						}, nil
					case int32(4):
						buf := make([]byte, 4)
						_, err := input.Body.Read(buf)
						Expect(err).ToNot(HaveOccurred())
						Expect(buf).To(Equal([]byte("5678")))
						return &awss3.UploadPartOutput{
							ETag: aws.String("etag-4"),
						}, nil
					case int32(5):
						buf := make([]byte, 4)
						_, err := input.Body.Read(buf)
						Expect(err).ToNot(HaveOccurred())
						Expect(buf).To(Equal([]byte("90AB")))
						return &awss3.UploadPartOutput{
							ETag: aws.String("etag-5"),
						}, nil
					default:
						Fail("unexpected part number")
						return nil, nil
					}
				}).Times(3)
			mockS3API.EXPECT().PutObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.PutObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.PutObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Metadata[multipartKeyMeta]))

					buf := make([]byte, 2)
					_, err := input.Body.Read(buf)
					Expect(err).ToNot(HaveOccurred())
					Expect(bytes.Equal(buf, []byte("CD"))).To(BeTrue())
					return nil, nil
				})

			bytesRead, err := destStorage.TransferFileChunk(
				ctx,
				fileInfo.Path, bytes.NewReader([]byte("1234567890ABCD")), 300, mockClient,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(bytesRead).To(Equal(int64(14)))
		}, NodeTimeout(10*time.Second))

		It("write chunk should write incomplete part because too small", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-1"),
						PartNumber: aws.Int32(1),
					},
					{
						Size:       aws.Int64(200),
						ETag:       aws.String("etag-2"),
						PartNumber: aws.Int32(2),
					},
				},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &types.NoSuchKey{})
			mockS3API.EXPECT().PutObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.PutObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.PutObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Metadata[multipartKeyMeta]))

					buf := make([]byte, 10)
					_, err := input.Body.Read(buf)
					Expect(err).ToNot(HaveOccurred())
					Expect(bytes.Equal(buf, []byte("1234567890"))).To(BeTrue())
					return nil, nil
				})

			bytesRead, err := destStorage.TransferFileChunk(
				ctx,
				fileInfo.Path, bytes.NewReader([]byte("1234567890")), 300, mockClient,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(bytesRead).To(Equal(int64(10)))
		}, NodeTimeout(10*time.Second))

		It("write chunk should prepends incomplete part", func(ctx context.Context) {
			destStorage = destStorageFactory(func(s *Destination) {
				s.MaxPartSize = 8
				s.MinPartSize = 4
				s.PreferredPartSize = 4
				s.MaxMultipartParts = 10000
				s.MaxObjectSize = 5 * 1024 * 1024 * 1024 * 1024
			})

			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Size = 5
					fileInfo.Offset = 0
					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(&awss3.HeadObjectOutput{
				ContentLength: aws.Int64(3),
			}, nil)
			mockS3API.EXPECT().GetObject(ctx, &awss3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(&awss3.GetObjectOutput{
				ContentLength: aws.Int64(3),
				Body:          io.NopCloser(bytes.NewReader([]byte("123"))),
			}, nil)
			mockS3API.EXPECT().DeleteObject(ctx, &awss3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(&awss3.DeleteObjectOutput{}, nil)

			mockS3API.EXPECT().UploadPart(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.UploadPartInput,
					opts ...func(*awss3.Options),
				) (*awss3.UploadPartOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Path))
					Expect(*input.UploadId).To(Equal("test-multipart-id"))

					switch *input.PartNumber {
					case int32(1):
						buf := make([]byte, 4)
						_, err := input.Body.Read(buf)
						Expect(err).ToNot(HaveOccurred())
						Expect(buf).To(Equal([]byte("1234")))
						return &awss3.UploadPartOutput{
							ETag: aws.String("etag-1"),
						}, nil
					case int32(2):
						buf := make([]byte, 1)
						_, err := input.Body.Read(buf)
						Expect(err).ToNot(HaveOccurred())
						Expect(buf).To(Equal([]byte("5")))
						return &awss3.UploadPartOutput{
							ETag: aws.String("etag-2"),
						}, nil
					default:
						Fail("unexpected part number")
						return nil, nil
					}
				}).Times(2)

			bytesRead, err := destStorage.TransferFileChunk(
				ctx,
				fileInfo.Path, bytes.NewReader([]byte("45")), 3, mockClient,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(bytesRead).To(Equal(int64(2)))
		}, NodeTimeout(10*time.Second))

		It("write chunk should prepends incomplete part and write a new incomplete part", func(ctx context.Context) {
			destStorage = destStorageFactory(func(s *Destination) {
				s.MaxPartSize = 8
				s.MinPartSize = 4
				s.PreferredPartSize = 4
				s.MaxMultipartParts = 10000
				s.MaxObjectSize = 5 * 1024 * 1024 * 1024 * 1024
			})

			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Size = 10
					fileInfo.Offset = 0
					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{Parts: []types.Part{}}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(&awss3.HeadObjectOutput{
				ContentLength: aws.Int64(3),
			}, nil)
			mockS3API.EXPECT().GetObject(ctx, &awss3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(&awss3.GetObjectOutput{
				ContentLength: aws.Int64(3),
				Body:          io.NopCloser(bytes.NewReader([]byte("123"))),
			}, nil)
			mockS3API.EXPECT().DeleteObject(ctx, &awss3.DeleteObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(&awss3.DeleteObjectOutput{}, nil)
			mockS3API.EXPECT().UploadPart(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.UploadPartInput,
					opts ...func(*awss3.Options),
				) (*awss3.UploadPartOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Path))
					Expect(*input.UploadId).To(Equal("test-multipart-id"))
					buf := make([]byte, 4)
					_, err := input.Body.Read(buf)
					Expect(err).ToNot(HaveOccurred())
					Expect(buf).To(Equal([]byte("1234")))
					return &awss3.UploadPartOutput{
						ETag: aws.String("etag-1"),
					}, nil
				})
			mockS3API.EXPECT().PutObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.PutObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.PutObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Metadata[multipartKeyMeta]))
					buf := make([]byte, 1)
					_, err := input.Body.Read(buf)
					Expect(err).ToNot(HaveOccurred())
					Expect(bytes.Equal(buf, []byte("5"))).To(BeTrue())
					return nil, nil
				})

			bytesRead, err := destStorage.TransferFileChunk(
				ctx,
				fileInfo.Path, bytes.NewReader([]byte("45")), 3, mockClient,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(bytesRead).To(Equal(int64(2)))
		}, NodeTimeout(10*time.Second))

		It("write chunk should allow too small last", func(ctx context.Context) {
			destStorage = destStorageFactory(func(s *Destination) {
				s.MinPartSize = 20
			})

			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Size = 500
					fileInfo.Offset = 0
					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						PartNumber: aws.Int32(1),
						Size:       aws.Int64(400),
						ETag:       aws.String("etag-1"),
					},
					{
						PartNumber: aws.Int32(2),
						Size:       aws.Int64(90),
						ETag:       aws.String("etag-2"),
					},
				},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied."})
			mockS3API.EXPECT().UploadPart(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.UploadPartInput,
					opts ...func(*awss3.Options),
				) (*awss3.UploadPartOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(fileInfo.Path))
					Expect(*input.UploadId).To(Equal("test-multipart-id"))
					Expect(*input.PartNumber).To(Equal(int32(3)))
					buf := make([]byte, 10)
					_, err := input.Body.Read(buf)
					Expect(err).ToNot(HaveOccurred())
					Expect(buf).To(Equal([]byte("1234567890")))
					return &awss3.UploadPartOutput{
						ETag: aws.String("etag-3"),
					}, nil
				})

			// 10 bytes are missing for the upload to be finished (offset at 490 for 500
			// bytes file) but the minimum chunk size is higher (20). The chunk is
			// still uploaded since the last part may be smaller than the minimum.
			bytesRead, err := destStorage.TransferFileChunk(
				ctx,
				fileInfo.Path, bytes.NewReader([]byte("1234567890")), 490, mockClient,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(bytesRead).To(Equal(int64(10)))
		}, NodeTimeout(10*time.Second))

		// This test ensures that the S3Store will clean up all files that it creates during
		// a call to WriteChunk, even if an error occurs during that invocation.
		// Here, we provide 14 bytes to WriteChunk and since the PartSize is set to 10,
		// it will split the input into two parts (10 bytes and 4 bytes).
		// Inside the first call to UploadPart, we assert that the temporary files
		// for both parts have been created, and we return an error.
		// In the end, we assert that the error bubbled up and that all temporary files have
		// been cleaned up.
		It("write chunk clean ups temp files", func(ctx context.Context) {
			// Create a temporary directory, so no files get mixed in.
			tempDir, err := os.MkdirTemp("", "file-transferer-s3-cleanup-tests-")
			Expect(err).ToNot(HaveOccurred())

			s3API := s3APIWithTempFileAssertion{
				MockS3API: mockS3API,
				tempDir:   tempDir,
			}
			destStorage = destStorageFactory(func(s *Destination) {
				s.MaxPartSize = 10
				s.MinPartSize = 10
				s.PreferredPartSize = 10
				s.MaxMultipartParts = 10000
				s.MaxObjectSize = 5 * 1024 * 1024 * 1024 * 1024
				s.TemporaryDirectory = tempDir
			})

			// The usual S3 calls for retrieving the upload
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetS3API().Return(s3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Size = 14
					fileInfo.Offset = 0
					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &types.NoSuchKey{})

			// No calls to mockS3API.EXPECT().UploadPart since that is handled by s3APIWithTempFileAssertion
			bytesRead, err := destStorage.TransferFileChunk(
				ctx,
				fileInfo.Path, bytes.NewReader([]byte("1234567890ABCD")), 0, mockClient,
			)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("not now"))
			Expect(bytesRead).To(Equal(int64(0)))

			files, err := os.ReadDir(tempDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(Equal(0))
		}, NodeTimeout(10*time.Second))
	})

	Describe("FinalizeTransfer", func() {
		It("should finish the upload successfully", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Size = 400
					fileInfo.Offset = 0
					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-1"),
						PartNumber: aws.Int32(1),
					},
					{
						Size:       aws.Int64(200),
						ETag:       aws.String("etag-2"),
						PartNumber: aws.Int32(2),
					},
				},
				NextPartNumberMarker: aws.String("2"),
				IsTruncated:          aws.Bool(true),
			}, nil)
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: aws.String("2"),
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-3"),
						PartNumber: aws.Int32(3),
					},
				},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &types.NotFound{})
			mockS3API.EXPECT().CompleteMultipartUpload(ctx, &awss3.CompleteMultipartUploadInput{
				Bucket:   aws.String(bucketName),
				Key:      &fileInfo.Path,
				UploadId: aws.String("test-multipart-id"),
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{
						{
							ETag:       aws.String("etag-1"),
							PartNumber: aws.Int32(1),
						},
						{
							ETag:       aws.String("etag-2"),
							PartNumber: aws.Int32(2),
						},
						{
							ETag:       aws.String("etag-3"),
							PartNumber: aws.Int32(3),
						},
					},
				},
			}).Return(nil, nil)
			mockS3API.EXPECT().PutObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.PutObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.PutObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))
					Expect(json.NewDecoder(input.Body).Decode(&xferfile.Info{})).To(Succeed())
					return nil, nil
				})

			Expect(destStorage.FinalizeTransfer(
				ctx,
				fileInfo.Path,
				mockClient,
			)).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should return error not finalize if total part size not equal source size", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))
					fileInfo.Size = 400
					fileInfo.Offset = 0
					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-1"),
						PartNumber: aws.Int32(1),
					},
				},
				NextPartNumberMarker: aws.String("2"),
				IsTruncated:          aws.Bool(true),
			}, nil)
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: aws.String("2"),
			}).Return(&awss3.ListPartsOutput{
				Parts: []types.Part{
					{
						Size:       aws.Int64(100),
						ETag:       aws.String("etag-3"),
						PartNumber: aws.Int32(3),
					},
				},
			}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &types.NotFound{})

			Expect(destStorage.FinalizeTransfer(
				ctx,
				fileInfo.Path,
				mockClient,
			)).To(MatchError(storage.ErrFileOrObjectCannotFinalize))
		}, NodeTimeout(10*time.Second))
	})

	Describe("DeleteFile", func() {
		It("should delete the file successfully", func(ctx context.Context) {
			connID := uuid.NewString()
			mockClient.EXPECT().GetConnectionID().
				Return(connID)
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			mockClient.EXPECT().GetCredential().
				Return(*s3ProtocClient)
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).
				DoAndReturn(func(
					ctx context.Context,
					input *awss3.GetObjectInput,
					opts ...func(*awss3.Options),
				) (*awss3.GetObjectOutput, error) {
					Expect(*input.Bucket).To(Equal(bucketName))
					Expect(*input.Key).To(Equal(infoPath))

					fileInfo.Metadata[bucketMeta] = bucketName
					fileInfo.Metadata[multipartIDMeta] = "test-multipart-id"
					fileInfo.Metadata[objectKeyMeta] = fileInfo.Path
					infoBytes, err := json.Marshal(fileInfo)
					Expect(err).ToNot(HaveOccurred())
					return &awss3.GetObjectOutput{
						Body: io.NopCloser(bytes.NewReader(infoBytes)),
					}, nil
				})
			mockS3API.EXPECT().ListParts(ctx, &awss3.ListPartsInput{
				Bucket:           aws.String(bucketName),
				Key:              &fileInfo.Path,
				UploadId:         aws.String("test-multipart-id"),
				PartNumberMarker: nil,
			}).Return(&awss3.ListPartsOutput{}, nil)
			mockS3API.EXPECT().HeadObject(ctx, &awss3.HeadObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(fileInfo.Metadata[multipartKeyMeta]),
			}).Return(nil, &types.NotFound{})
			mockS3API.EXPECT().AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucketName),
				Key:      &fileInfo.Path,
				UploadId: aws.String("test-multipart-id"),
			}).Return(nil, nil)

			mockS3API.EXPECT().DeleteObjects(ctx, &awss3.DeleteObjectsInput{
				Bucket: aws.String(bucketName),
				Delete: &types.Delete{
					Objects: []types.ObjectIdentifier{
						{
							Key: aws.String(fileInfo.Path),
						},
						{
							Key: aws.String(fileInfo.Metadata[multipartKeyMeta]),
						},
						{
							Key: aws.String(infoPath),
						},
					},
					Quiet: aws.Bool(true),
				},
			}).Return(&awss3.DeleteObjectsOutput{}, nil)

			Expect(destStorage.DeleteFile(
				ctx,
				fileInfo.Path,
				mockClient,
			)).To(Succeed())
		}, NodeTimeout(10*time.Second))
	})
})

type s3APIWithTempFileAssertion struct {
	*mock_protoc.MockS3API
	tempDir string
}

func (s s3APIWithTempFileAssertion) UploadPart(
	context.Context,
	*awss3.UploadPartInput,
	...func(*awss3.Options),
) (*awss3.UploadPartOutput, error) {
	GinkgoHelper()
	// Make sure that there are temporary files from transfer in here.
	files, err := os.ReadDir(s.tempDir)
	Expect(err).ToNot(HaveOccurred())
	for _, file := range files {
		Expect(strings.HasPrefix(file.Name(), "file-transferer-s3-tmp-")).To(BeTrue())
	}

	Expect(len(files)).To(BeNumerically(">=", 1))
	Expect(len(files)).To(BeNumerically("<=", 3))

	return nil, fmt.Errorf("not now")
}
