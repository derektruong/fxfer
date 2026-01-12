package local_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/derektruong/fxfer/protoc/ftp"
	local_protoc "github.com/derektruong/fxfer/protoc/local"
	"github.com/derektruong/fxfer/storage/local"
)

var _ = Describe("Source", func() {
	var (
		err         error
		srcStorage  *local.Source
		testContent string
	)

	BeforeEach(func() {
		srcStorage, err = local.NewSource(GinkgoLogr)
		Expect(err).ToNot(HaveOccurred())
		DeferCleanup(srcStorage.Close)
		testContent = gofakeit.SentenceSimple()
	})

	Describe("GetFileInfo", func() {
		It("should return error if protocol is not local", func(ctx context.Context) {
			_, err := srcStorage.GetFileInfo(
				ctx,
				"test",
				ftp.NewClient(
					"test",
					1234,
					"test",
					"test",
				),
			)
			Expect(err).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should return correct file info", func(ctx context.Context) {
			filePath := filepath.Join(tempDir, "test-abc.txt")
			writeSourceFileContent(filePath, testContent)
			info, err := srcStorage.GetFileInfo(ctx, filePath, local_protoc.NewIO())
			Expect(err).ToNot(HaveOccurred())
			Expect(info).To(And(
				HaveField("Path", filePath),
				HaveField("Name", "test-abc"),
				HaveField("Extension", "txt"),
				HaveField("Size", int64(len(testContent))),
				HaveField("ModTime", Not(BeZero())),
			))
		}, NodeTimeout(10*time.Second))

		It("should return error if file does not exist", func(ctx context.Context) {
			_, err := srcStorage.GetFileInfo(ctx, "test", local_protoc.NewIO())
			Expect(errors.Is(err, fs.ErrNotExist)).To(BeTrue())
		})
	})

	Describe("GetFileFromOffset", func() {
		It("should return error if protocol is not local", func(ctx context.Context) {
			_, err := srcStorage.GetFileFromOffset(
				ctx,
				"test",
				0,
				ftp.NewClient(
					"test",
					1234,
					"test",
					"test",
				),
			)
			Expect(err).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should return correct file content", func(ctx context.Context) {
			filePath := filepath.Join(tempDir, "test-abc.txt")
			writeSourceFileContent(filePath, testContent)
			reader, err := srcStorage.GetFileFromOffset(ctx, filePath, 0, local_protoc.NewIO())
			Expect(err).ToNot(HaveOccurred())
			defer reader.Close()
			content, err := io.ReadAll(reader)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(content)).To(Equal(testContent))
		}, NodeTimeout(10*time.Second))

		It("should return correct file content from offset", func(ctx context.Context) {
			filePath := filepath.Join(tempDir, "test-abc.txt")
			writeSourceFileContent(filePath, testContent)
			reader, err := srcStorage.GetFileFromOffset(ctx, filePath, 5, local_protoc.NewIO())
			Expect(err).ToNot(HaveOccurred())
			defer reader.Close()
			content, err := io.ReadAll(reader)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(content)).To(Equal(testContent[5:]))
		}, NodeTimeout(10*time.Second))

		It("should return error if file does not exist", func(ctx context.Context) {
			_, err := srcStorage.GetFileFromOffset(ctx, "test", 0, local_protoc.NewIO())
			Expect(errors.Is(err, fs.ErrNotExist)).To(BeTrue())
		})
	})
})

func writeSourceFileContent(filePath string, content string) {
	ExpectWithOffset(
		1,
		os.WriteFile(filePath, []byte(content), 0644),
	).To(Succeed())
}
