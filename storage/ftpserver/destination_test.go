package ftpserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"io"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc/ftp"
	mock_protoc "github.com/derektruong/fxfer/protoc/mock"
	"github.com/derektruong/fxfer/storage"
	"github.com/derektruong/fxfer/storage/ftpserver"
	"strings"
	"time"
)

var _ = Describe("Destination", func() {
	var (
		mockCtrl         *gomock.Controller
		mockFTPPool      *mock_protoc.MockConnectionPool
		mockClient       *mock_protoc.MockClient
		destStorage      *ftpserver.Destination
		ftpClient        ftp.Client
		connID, filePath string
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockFTPPool = mock_protoc.NewMockConnectionPool(mockCtrl)
		mockClient = mock_protoc.NewMockClient(mockCtrl)
		destStorage = ftpserver.NewDestination(GinkgoLogr)
		DeferCleanup(func() {
			mockFTPPool.EXPECT().Close().AnyTimes()
			destStorage.Close()
		})
		connID = uuid.NewString()
		ftpClient = ftp.Client{
			Host:     "localhost",
			Port:     21,
			Username: "test",
			Password: "test",
		}
	})

	Describe("GetFileInfo", func() {
		It("should return error not exists if not found when getting file size", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			filePath = "test.txt"
			mockFTPPool.EXPECT().GetFileSizeAndModTime(ctx, filePath).
				Return(int64(0), time.Time{}, xferfile.ErrFileNotExists)

			_, err := destStorage.GetFileInfo(ctx, filePath, mockClient)
			Expect(err).To(MatchError(xferfile.ErrFileNotExists))
		}, NodeTimeout(10*time.Second))

		It("should return error not exists if not found when getting file info", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			filePath = "folder1/folder2/test-ductruong.txt"
			modTime := time.Now()

			mockFTPPool.EXPECT().GetFileSizeAndModTime(ctx, filePath).
				Return(int64(100), modTime, nil)
			mockFTPPool.EXPECT().RetrieveFileFromOffset(ctx, gomock.Any(), int64(0)).
				DoAndReturn(func(
					ctx context.Context,
					filePath string,
					offset int64,
				) (reader io.ReadCloser, err error) {
					Expect(strings.HasSuffix(filePath, ".info")).To(BeTrue())
					return nil, xferfile.ErrFileNotExists
				})

			_, err := destStorage.GetFileInfo(ctx, filePath, mockClient)
			Expect(err).To(MatchError(xferfile.ErrFileNotExists))
		}, NodeTimeout(10*time.Second))

		It("should return correct file info", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			filePath = "folder1/folder2/test.txt"
			modTime := time.Now()
			mockFTPPool.EXPECT().GetFileSizeAndModTime(ctx, filePath).
				Return(int64(100), modTime, nil)
			mockFTPPool.EXPECT().RetrieveFileFromOffset(ctx, gomock.Any(), int64(0)).
				DoAndReturn(func(
					ctx context.Context,
					filePath string,
					offset int64,
				) (reader io.ReadCloser, err error) {
					Expect(strings.HasSuffix(filePath, ".info")).To(BeTrue())
					return io.NopCloser(strings.NewReader(`{"path":"folder1/folder2/test.txt","size":100,"name":"test","extension":"txt","modTime":"` + modTime.Format(time.RFC3339) + `"}`)), nil
				})

			info, err := destStorage.GetFileInfo(ctx, filePath, mockClient)
			Expect(err).To(Not(HaveOccurred()))
			Expect(info.Path).To(Equal(filePath))
			Expect(info.Name).To(Equal("test"))
			Expect(info.Extension).To(Equal("txt"))
			Expect(info.Size).To(Equal(int64(100)))
			Expect(info.ModTime).To(BeTemporally("~", modTime, time.Second))
		}, NodeTimeout(10*time.Second))
	})

	Describe("CreateFile", func() {
		It("should create the file successfully", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			modTime := gofakeit.PastDate()
			filePath := "/tmp/test-xyz-1.txt"
			infoPath, err := xferfile.GenerateInfoPath(filePath)
			Expect(err).ToNot(HaveOccurred())

			mockFTPPool.EXPECT().MakeDirectoryAll(ctx, "/tmp").Return(nil)
			mockFTPPool.EXPECT().
				CreateOrOverwriteFile(ctx, filePath, gomock.Any()).Return(nil)
			mockFTPPool.EXPECT().
				CreateOrOverwriteFile(ctx, infoPath, gomock.Any()).
				Do(func(ctx context.Context, gotPath string, reader io.Reader) {
					Expect(strings.HasSuffix(gotPath, ".info")).To(BeTrue())
					var fileInfo xferfile.Info
					Expect(json.NewDecoder(reader).Decode(&fileInfo)).To(Succeed())
					Expect(fileInfo.Path).To(Equal(filePath))
					Expect(fileInfo.Name).To(Equal("test-xyz-1"))
					Expect(fileInfo.Extension).To(Equal("txt"))
					Expect(fileInfo.Size).To(Equal(int64(10000)))
					Expect(fileInfo.Offset).To(Equal(int64(0)))
					Expect(fileInfo.ModTime).To(BeTemporally("~", modTime, time.Second))
				}).Return(nil)

			Expect(destStorage.CreateFile(
				ctx,
				filePath, 10000, modTime,
				mockClient,
			)).To(Succeed())
		}, NodeTimeout(10*time.Second))
	})

	Describe("TransferFileChunk", Ordered, func() {
		var testChunkContent string

		BeforeAll(func() {
			filePath = "/fol-xyz-1/test-xyz-1.txt"
			testChunkContent = "aabbbcccc"
		})

		It("should init transfer the file chunk successfully", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(3)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			modTime := gofakeit.PastDate()
			mockFTPPool.EXPECT().MakeDirectoryAll(ctx, "/fol-xyz-1").Return(nil)
			mockFTPPool.EXPECT().
				CreateOrOverwriteFile(ctx, filePath, gomock.Any()).Return(nil)
			mockFTPPool.EXPECT().
				CreateOrOverwriteFile(ctx, gomock.Any(), gomock.Any()).Return(nil)
			Expect(destStorage.CreateFile(
				ctx,
				filePath, int64(len(testChunkContent)), modTime,
				mockClient,
			)).To(Succeed())

			mockFTPPool.EXPECT().
				AppendToFile(ctx, filePath, gomock.Any(), int64(0)).
				Do(func(ctx context.Context, gotPath string, reader io.Reader, offset int64) {
					Expect(gotPath).To(Equal(filePath))
					buf := new(bytes.Buffer)
					n, err := buf.ReadFrom(reader)
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(int64(2)))
					Expect(buf.String()).To(Equal(testChunkContent[:2]))
				}).Return(nil)

			By("transfer the file chunk")
			_, err := destStorage.TransferFileChunk(
				ctx,
				filePath,
				bytes.NewReader([]byte(testChunkContent[:2])),
				0,
				mockClient,
			)
			Expect(err).ToNot(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should append the file chunk successfully", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(4)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			By("transfer the file chunk")
			mockFTPPool.EXPECT().AppendToFile(ctx, filePath, gomock.Any(), int64(2)).
				Do(func(ctx context.Context, gotPath string, reader io.Reader, offset int64) {
					Expect(gotPath).To(Equal(filePath))
					buf := new(bytes.Buffer)
					n, err := buf.ReadFrom(reader)
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(int64(3)))
					Expect(buf.String()).To(Equal(testChunkContent[2:5]))
				}).Return(nil)
			_, err := destStorage.TransferFileChunk(
				ctx,
				filePath,
				bytes.NewReader([]byte(testChunkContent[2:5])),
				2,
				mockClient,
			)
			Expect(err).ToNot(HaveOccurred())

			By("transfer the file chunk")
			mockFTPPool.EXPECT().AppendToFile(ctx, filePath, gomock.Any(), int64(5)).
				Do(func(ctx context.Context, gotPath string, reader io.Reader, offset int64) {
					Expect(gotPath).To(Equal(filePath))
					buf := new(bytes.Buffer)
					n, err := buf.ReadFrom(reader)
					Expect(err).ToNot(HaveOccurred())
					Expect(n).To(Equal(int64(len(testChunkContent) - 5)))
					Expect(buf.String()).To(Equal(testChunkContent[5:]))
				}).Return(nil)

			_, err = destStorage.TransferFileChunk(
				ctx,
				filePath,
				bytes.NewReader([]byte(testChunkContent[5:])),
				5,
				mockClient,
			)
			Expect(err).ToNot(HaveOccurred())
		}, NodeTimeout(10*time.Second))
	})

	Describe("FinalizeTransfer", func() {
		It("should finalize the transfer successfully", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			filePath = "/fol-xyz-1/test-xyz-1.txt"
			modTime := gofakeit.PastDate()
			mockFTPPool.EXPECT().GetFileSizeAndModTime(ctx, filePath).
				Return(int64(100), modTime, nil)
			mockFTPPool.EXPECT().RetrieveFileFromOffset(ctx, gomock.Any(), int64(0)).
				DoAndReturn(func(
					ctx context.Context,
					filePath string,
					offset int64,
				) (reader io.ReadCloser, err error) {
					Expect(strings.HasSuffix(filePath, ".info")).To(BeTrue())
					return io.NopCloser(strings.NewReader(`{"path":"` + filePath + `","size":100,"name":"test-xyz-1","extension":"txt","modTime":"` + modTime.Format(time.RFC3339) + `"}`)), nil
				})

			infoPath, err := xferfile.GenerateInfoPath(filePath)
			Expect(err).ToNot(HaveOccurred())
			mockFTPPool.EXPECT().
				CreateOrOverwriteFile(ctx, infoPath, gomock.Any()).Return(nil)

			Expect(destStorage.FinalizeTransfer(
				ctx,
				filePath,
				mockClient,
			)).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should return error file cannot finalize if offset is not equal to size", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).Return(nil)

			filePath = "/fol-xyz-1/test-xyz-1.txt"
			modTime := gofakeit.PastDate()
			mockFTPPool.EXPECT().GetFileSizeAndModTime(ctx, filePath).
				Return(int64(50), modTime, nil)
			mockFTPPool.EXPECT().RetrieveFileFromOffset(ctx, gomock.Any(), int64(0)).
				DoAndReturn(func(
					ctx context.Context,
					filePath string,
					offset int64,
				) (reader io.ReadCloser, err error) {
					Expect(strings.HasSuffix(filePath, ".info")).To(BeTrue())
					return io.NopCloser(strings.NewReader(`{"path":"` + filePath + `","size":100,"name":"test-xyz-1","extension":"txt","modTime":"` + modTime.Format(time.RFC3339) + `"}`)), nil
				})

			Expect(destStorage.FinalizeTransfer(
				ctx,
				filePath,
				mockClient,
			)).To(MatchError(storage.ErrFileOrObjectCannotFinalize))
		}, NodeTimeout(10*time.Second))
	})

	Describe("DeleteFile", func() {
		It("should delete the file successfully", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftpClient).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			filePath = "/fol-xyz-1/test-xyz-1.txt"
			mockFTPPool.EXPECT().DeleteFile(ctx, filePath).Return(nil)

			infoPath, err := xferfile.GenerateInfoPath(filePath)
			Expect(err).ToNot(HaveOccurred())
			mockFTPPool.EXPECT().DeleteFile(ctx, infoPath).Return(nil)

			Expect(destStorage.DeleteFile(
				ctx,
				filePath,
				mockClient,
			)).To(Succeed())
		}, NodeTimeout(10*time.Second))
	})
})
