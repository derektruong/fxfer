package s3

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/derektruong/fxfer/internal/xferfile"
	localio_protoc "github.com/derektruong/fxfer/protoc/local"
	mock_protoc "github.com/derektruong/fxfer/protoc/mock"
	s3_protoc "github.com/derektruong/fxfer/protoc/s3"
	"github.com/derektruong/fxfer/storage"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("Source", func() {
	var (
		err        error
		filePath   string
		mockCtrl   *gomock.Controller
		mockS3API  *mock_protoc.MockS3API
		mockClient *mock_protoc.MockClient
		srcStorage *Source
	)

	BeforeEach(func(ctx context.Context) {
		mockCtrl = gomock.NewController(GinkgoT())
		DeferCleanup(mockCtrl.Finish)
		mockS3API = mock_protoc.NewMockS3API(mockCtrl)
		mockClient = mock_protoc.NewMockClient(mockCtrl)

		filePath = "dt-folder/dt-large-file.xmf"
		content := "Lorem ipsum dolor sit amet, consectetur adipiscing elit."

		_, err = awsS3Client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:        aws.String(bucketName),
			Key:           &filePath,
			Body:          strings.NewReader(content),
			ContentLength: aws.Int64(int64(len(content))),
		})
		Expect(err).ToNot(HaveOccurred())

		srcStorage = NewSource(GinkgoLogr)
		DeferCleanup(srcStorage.Close)
	}, NodeTimeout(10*time.Second))

	Describe("GetFileInfo", func() {
		It("should return the file info already exists", func(ctx context.Context) {
			var info xferfile.Info
			info, err = srcStorage.GetFileInfo(ctx, filePath, protocS3Client)
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Path).To(Equal(filePath))
			Expect(info.Extension).To(Equal("xmf"))
			Expect(info.ModTime).ToNot(BeZero())
		}, NodeTimeout(10*time.Second))

		It("should return err 404 if the file info does not exist", func(ctx context.Context) {
			_, err = srcStorage.GetFileInfo(ctx, "dt-folder/dt-large-file-not-exist.xmf", protocS3Client)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("404"))
		}, NodeTimeout(10*time.Second))

		It("should return error when checking and setting client failed", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return("")
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			wrongClient := localio_protoc.NewIO()
			mockClient.EXPECT().GetCredential().
				Return(*wrongClient)

			_, err = srcStorage.GetFileInfo(ctx, filePath, mockClient)
			Expect(err).To(MatchError(storage.ErrS3ProtocolClientInvalid))
		}, NodeTimeout(10*time.Second))

		It("should return error when extracting file info", func(ctx context.Context) {
			wrongFilePath := "dt-folder/dt-large-file"
			_, err = awsS3Client.PutObject(ctx, &awss3.PutObjectInput{
				Bucket:        aws.String(bucketName),
				Key:           &wrongFilePath,
				Body:          strings.NewReader("Lorem ipsum dolor sit amet, consectetur adipiscing elit."),
				ContentLength: aws.Int64(int64(len("Lorem ipsum dolor sit amet, consectetur adipiscing elit."))),
			})
			Expect(err).ToNot(HaveOccurred())
			_, err = srcStorage.GetFileInfo(ctx, wrongFilePath, protocS3Client)
			Expect(err).To(MatchError("file extension is required"))
		}, NodeTimeout(10*time.Second))
	})

	Describe("GetFileFromOffset", func() {
		It("should return the file content from the offset", func(ctx context.Context) {
			var reader io.ReadCloser
			reader, err = srcStorage.GetFileFromOffset(ctx, filePath, 0, protocS3Client)
			Expect(err).ToNot(HaveOccurred())
			defer reader.Close()

			content, err := io.ReadAll(reader)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(content)).To(Equal("Lorem ipsum dolor sit amet, consectetur adipiscing elit."))
		}, NodeTimeout(10*time.Second))

		It("should return the file content from the offset", func(ctx context.Context) {
			var reader io.ReadCloser
			reader, err = srcStorage.GetFileFromOffset(ctx, filePath, 12, protocS3Client)
			Expect(err).ToNot(HaveOccurred())
			defer reader.Close()

			content, err := io.ReadAll(reader)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(content)).To(Equal("dolor sit amet, consectetur adipiscing elit."))
		}, NodeTimeout(10*time.Second))

		It("should return error when checking and setting client failed", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return("")
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			wrongClient := localio_protoc.NewIO()
			mockClient.EXPECT().GetCredential().
				Return(*wrongClient)

			_, err = srcStorage.GetFileFromOffset(ctx, filePath, 0, mockClient)
			Expect(err).To(MatchError(storage.ErrS3ProtocolClientInvalid))
		}, NodeTimeout(10*time.Second))

		It("should return error when getting file object", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return("")
			mockClient.EXPECT().GetS3API().Return(mockS3API)
			s3ProtocClient := s3_protoc.NewClient(endpoint, bucketName, region, accessKey, secretKey)
			mockClient.EXPECT().GetCredential().Return(*s3ProtocClient)

			occurError := gofakeit.Error()
			mockS3API.EXPECT().GetObject(ctx, gomock.Any()).Return(nil, occurError)

			_, err = srcStorage.GetFileFromOffset(ctx, filePath, 0, mockClient)
			Expect(err).To(MatchError(occurError))
		}, NodeTimeout(10*time.Second))
	})
})
