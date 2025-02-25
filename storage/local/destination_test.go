package local_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/derektruong/fxfer/internal/xferfile"
	local_protoc "github.com/derektruong/fxfer/protoc/local"
	"github.com/derektruong/fxfer/storage"
	"github.com/derektruong/fxfer/storage/local"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Destination", func() {
	var (
		err         error
		destStorage *local.Destination
		localProtoc *local_protoc.IO
		testContent string
	)

	BeforeEach(func() {
		destStorage, err = local.NewDestination(GinkgoLogr)
		Expect(err).ToNot(HaveOccurred())
		DeferCleanup(destStorage.Close)
		localProtoc = local_protoc.NewIO()
		testContent = gofakeit.SentenceSimple()
	})

	Describe("GetFileInfo", func() {
		It("should return error if protocol is not local", func(ctx context.Context) {
			_, err := destStorage.GetFileInfo(
				ctx,
				"test",
				local_protoc.NewIO(),
			)
			Expect(err).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should return error if file does not exist", func(ctx context.Context) {
			_, err := destStorage.GetFileInfo(ctx, "test.png", localProtoc)
			Expect(err).To(MatchError(xferfile.ErrFileNotExists))
		}, NodeTimeout(10*time.Second))

		It("should return correct file info", func(ctx context.Context) {
			filePath := tempDir + "/test-abc-1.txt"
			writeDestFileContent(filePath, xferfile.Info{
				Path:      filePath,
				Size:      int64(len(testContent) + 100),
				Name:      "test-abc-1",
				Extension: "txt",
				ModTime:   gofakeit.PastDate(),
			}, testContent)
			info, err := destStorage.GetFileInfo(ctx, filePath, local_protoc.NewIO())
			Expect(err).ToNot(HaveOccurred())
			Expect(info).To(And(
				HaveField("Path", filePath),
				HaveField("Name", "test-abc-1"),
				HaveField("Extension", "txt"),
				HaveField("Size", int64(len(testContent)+100)),
				HaveField("Offset", int64(len(testContent))),
				HaveField("ModTime", Not(BeZero())),
			))
		}, NodeTimeout(10*time.Second))
	})

	Describe("CreateFile", func() {
		It("should return error if protocol is not local", func(ctx context.Context) {
			err := destStorage.CreateFile(
				ctx,
				"test",
				1234,
				gofakeit.PastDate(),
				local_protoc.NewIO(),
			)
			Expect(err).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should make dir all if not exists", func(ctx context.Context) {
			filePath := tempDir + "/test-abc-3/test-abc-3.txt"
			modTime := gofakeit.PastDate()
			Expect(destStorage.CreateFile(
				ctx,
				filePath, 10000, modTime,
				localProtoc,
			)).To(Succeed())

			By("assert the folder exists")
			infoFS, err := os.Stat(tempDir + "/test-abc-3")
			Expect(err).ToNot(HaveOccurred())
			Expect(infoFS.IsDir()).To(BeTrue())
		}, NodeTimeout(10*time.Second))

		It("should create the file successfully", func(ctx context.Context) {
			filePath := tempDir + "/test-abc-2.txt"
			modTime := gofakeit.PastDate()
			Expect(destStorage.CreateFile(
				ctx,
				filePath, 10000, modTime,
				localProtoc,
			)).To(Succeed())
			info, err := destStorage.GetFileInfo(ctx, filePath, local_protoc.NewIO())
			Expect(err).ToNot(HaveOccurred())

			By("assert the file info")
			Expect(info).To(And(
				HaveField("Path", filePath),
				HaveField("Name", "test-abc-2"),
				HaveField("Extension", "txt"),
				HaveField("Size", int64(10000)),
				HaveField("Offset", int64(0)),
				HaveField("ModTime", BeTemporally("~", modTime, time.Second)),
			))

			By("assert the file exists")
			infoFS, err := os.Stat(filePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(infoFS.Name()).To(Equal(info.Name + "." + info.Extension))
		}, NodeTimeout(10*time.Second))
	})

	Describe("TransferFileChunk", Ordered, func() {
		var filePath string
		var testChunkContent string

		BeforeAll(func() {
			filePath = tempDir + "/test-abc-4.txt"
			testChunkContent = "aabbbcccc"
		})

		It("should return error if protocol is not local", func(ctx context.Context) {
			_, err := destStorage.TransferFileChunk(
				ctx,
				"test",
				bytes.NewReader([]byte("test")),
				0,
				local_protoc.NewIO(),
			)
			Expect(err).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should init transfer the file chunk successfully", func(ctx context.Context) {
			modTime := gofakeit.PastDate()
			Expect(destStorage.CreateFile(
				ctx,
				filePath, int64(len(testChunkContent)), modTime,
				localProtoc,
			)).To(Succeed())

			By("transfer the file chunk")
			n, err := destStorage.TransferFileChunk(
				ctx,
				filePath,
				bytes.NewReader([]byte(testChunkContent[:2])),
				0,
				localProtoc,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(int64(2)))

			By("assert the file size")
			info, err := destStorage.GetFileInfo(ctx, filePath, localProtoc)
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Size).To(Equal(int64(len(testChunkContent))))
			Expect(info.Offset).To(Equal(int64(2)))
		}, NodeTimeout(10*time.Second))

		It("should append the file chunk successfully", func(ctx context.Context) {
			By("transfer the file chunk")
			n, err := destStorage.TransferFileChunk(
				ctx,
				filePath,
				bytes.NewReader([]byte(testChunkContent[2:5])),
				2,
				localProtoc,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(int64(3)))

			By("assert the file size")
			info, err := destStorage.GetFileInfo(ctx, filePath, localProtoc)
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Size).To(Equal(int64(len(testChunkContent))))
			Expect(info.Offset).To(Equal(int64(5)))

			By("transfer the file chunk")
			n, err = destStorage.TransferFileChunk(
				ctx,
				filePath,
				bytes.NewReader([]byte(testChunkContent[5:])),
				5,
				localProtoc,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(Equal(int64(len(testChunkContent) - 5)))

			By("assert the file size")
			info, err = destStorage.GetFileInfo(ctx, filePath, localProtoc)
			Expect(err).ToNot(HaveOccurred())
			Expect(info.Size).To(Equal(int64(len(testChunkContent))))
			Expect(info.Offset).To(Equal(int64(len(testChunkContent))))

			By("assert the file content")
			buf := new(bytes.Buffer)
			f, err := os.Open(filePath)
			Expect(err).ToNot(HaveOccurred())
			defer f.Close()
			_, err = io.Copy(buf, f)
			Expect(err).ToNot(HaveOccurred())
			Expect(buf.String()).To(Equal(testChunkContent))
		}, NodeTimeout(10*time.Second))
	})

	Describe("FinalizeTransfer", func() {
		var filePath string

		BeforeEach(func() {
			filePath = tempDir + "/test-abc-5.txt"
		})

		It("should return error if protocol is not local", func(ctx context.Context) {
			err := destStorage.FinalizeTransfer(
				ctx,
				"test",
				local_protoc.NewIO(),
			)
			Expect(err).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should finalize the transfer successfully", func(ctx context.Context) {
			modTime := gofakeit.PastDate()
			Expect(destStorage.CreateFile(
				ctx,
				filePath, int64(len(testContent)), modTime,
				localProtoc,
			)).To(Succeed())

			_, err = destStorage.TransferFileChunk(
				ctx,
				filePath,
				bytes.NewReader([]byte(testContent)),
				0,
				localProtoc,
			)
			Expect(err).ToNot(HaveOccurred())

			By("finalize the transfer")
			Expect(destStorage.FinalizeTransfer(ctx, filePath, localProtoc)).To(Succeed())

			By("assert the file info")
			info, err := destStorage.GetFileInfo(ctx, filePath, localProtoc)
			Expect(err).ToNot(HaveOccurred())
			Expect(info.FinishTime).ToNot(BeZero())
		}, NodeTimeout(10*time.Second))

		It("should return error if file cannot finalize", func(ctx context.Context) {
			modTime := gofakeit.PastDate()
			Expect(destStorage.CreateFile(
				ctx,
				filePath, int64(len(testContent)), modTime,
				localProtoc,
			)).To(Succeed())

			By("finalize the transfer")
			Expect(destStorage.FinalizeTransfer(ctx, filePath, localProtoc)).
				To(MatchError(storage.ErrFileOrObjectCannotFinalize))
		}, NodeTimeout(10*time.Second))
	})

	Describe("DeleteFile", func() {
		var filePath string

		BeforeEach(func() {
			filePath = tempDir + "/test-abc-6.txt"
		})

		It("should return error if protocol is not local", func(ctx context.Context) {
			err := destStorage.DeleteFile(
				ctx,
				"test",
				local_protoc.NewIO(),
			)
			Expect(err).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should delete the file successfully", func(ctx context.Context) {
			modTime := gofakeit.PastDate()
			Expect(destStorage.CreateFile(
				ctx,
				filePath, int64(len(testContent)), modTime,
				localProtoc,
			)).To(Succeed())

			By("delete the file")
			Expect(destStorage.DeleteFile(ctx, filePath, localProtoc)).To(Succeed())

			By("assert the file does not exist")
			_, err := os.Stat(filePath)
			Expect(os.IsNotExist(err)).To(BeTrue())

			By("assert the file info does not exist")
			_, err = os.Stat(filePath + ".info")
			Expect(os.IsNotExist(err)).To(BeTrue())
		}, NodeTimeout(10*time.Second))
	})
})

func writeDestFileContent(filePath string, fileInfo xferfile.Info, content string) {
	GinkgoHelper()
	ExpectWithOffset(
		1,
		os.WriteFile(filePath, []byte(content), 0644),
	).To(Succeed())
	infoPath, err := xferfile.GenerateInfoPath(filePath)
	Expect(err).ToNot(HaveOccurred())
	infoData, err := json.Marshal(fileInfo)
	Expect(err).ToNot(HaveOccurred())
	ExpectWithOffset(
		1,
		os.WriteFile(infoPath, infoData, 0644),
	).To(Succeed())
}
