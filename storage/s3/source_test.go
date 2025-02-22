package s3

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/derektruong/fxfer/internal/xferfile"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Source", func() {
	var (
		err        error
		filePath   string
		srcStorage *Source
	)

	BeforeEach(func(ctx context.Context) {
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
	})
})
