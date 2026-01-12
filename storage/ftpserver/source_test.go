package ftpserver_test

import (
	"bytes"
	"context"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"io"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/ftp"
	mock_protoc "github.com/derektruong/fxfer/protoc/mock"
	"github.com/derektruong/fxfer/storage/ftpserver"
	"time"
)

var _ = Describe("Source", func() {
	var (
		mockCtrl    *gomock.Controller
		mockFTPPool *mock_protoc.MockConnectionPool
		mockClient  *mock_protoc.MockClient
		srcStorage  *ftpserver.Source
		connID      string
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		DeferCleanup(mockCtrl.Finish)
		mockFTPPool = mock_protoc.NewMockConnectionPool(mockCtrl)
		mockClient = mock_protoc.NewMockClient(mockCtrl)
		srcStorage = ftpserver.NewSource(GinkgoLogr)
		DeferCleanup(func() {
			mockFTPPool.EXPECT().Close().AnyTimes()
			srcStorage.Close()
		})
		connID = uuid.NewString()
	})

	Describe("GetFileInfo", func() {
		It("should return error if protocol not FTP client invalid", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(nil)
			_, err := srcStorage.GetFileInfo(ctx, "test", mockClient)
			Expect(err).To(MatchError(protoc.ErrProtocolInvalid(protoc.FTPProtoc)))
		}, NodeTimeout(10*time.Second))

		It("should return correct file info", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftp.Client{
				Host:     "test",
				Port:     1234,
				Username: "test",
				Password: "test",
			}).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			filePath := "folder1/folder2/test.txt"
			modTime := time.Now()
			mockFTPPool.EXPECT().GetFileSizeAndModTime(ctx, filePath).
				Return(int64(100), modTime, nil)
			info, err := srcStorage.GetFileInfo(ctx, filePath, mockClient)
			Expect(err).To(Not(HaveOccurred()))
			Expect(info.Path).To(Equal(filePath))
			Expect(info.Name).To(Equal("test"))
			Expect(info.Extension).To(Equal("txt"))
			Expect(info.Size).To(Equal(int64(100)))
			Expect(info.ModTime).To(BeTemporally("~", modTime, time.Second))
		}, NodeTimeout(10*time.Second))
	})

	Describe("GetFileFromOffset", func() {
		It("should return error if protocol not FTP client invalid", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(nil)
			_, err := srcStorage.GetFileFromOffset(ctx, "test", 0, mockClient)
			Expect(err).To(MatchError(protoc.ErrProtocolInvalid(protoc.FTPProtoc)))
		}, NodeTimeout(10*time.Second))

		It("should return reader", func(ctx context.Context) {
			mockClient.EXPECT().GetConnectionID().Return(connID).Times(2)
			mockClient.EXPECT().GetConnectionPool(gomock.Any()).Return(mockFTPPool)
			mockClient.EXPECT().GetCredential().Return(ftp.Client{
				Host:     "test",
				Port:     1234,
				Username: "test",
				Password: "test",
			}).Times(2)
			mockFTPPool.EXPECT().InitializeIdleConnection(gomock.Any()).
				Return(nil)

			filePath := "folder1/folder2/test.txt"
			reader := io.NopCloser(bytes.NewReader([]byte("Cây muốn lặng mà gió chẳng dừng.")))
			mockFTPPool.EXPECT().RetrieveFileFromOffset(ctx, filePath, int64(0)).
				Return(reader, nil)
			gotReader, err := srcStorage.GetFileFromOffset(ctx, filePath, 0, mockClient)
			Expect(err).To(Not(HaveOccurred()))
			buf := new(bytes.Buffer)
			_, err = buf.ReadFrom(gotReader)
			Expect(err).To(Not(HaveOccurred()))
			Expect(buf.String()).To(Equal("Cây muốn lặng mà gió chẳng dừng."))
		}, NodeTimeout(10*time.Second))
	})
})
