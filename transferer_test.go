package fxfer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/derektruong/fxfer"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/internal/xferfile/xferfiletest"
	"github.com/derektruong/fxfer/protoc"
	mock_protoc "github.com/derektruong/fxfer/protoc/mock"
	"github.com/derektruong/fxfer/storage"
	mock_storage "github.com/derektruong/fxfer/storage/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("transferer", func() {
	var (
		mockCtrl          *gomock.Controller
		mockClient        *mock_protoc.MockClient
		mockSrcStorage    *mock_storage.MockSource
		mockDestStorage   *mock_storage.MockDestination
		tfr               fxfer.Transferer
		srcCommand        fxfer.SourceCommand
		destCommand       fxfer.DestinationCommand
		callback          fxfer.ProgressUpdatedCallback
		srcInfo, destInfo xferfile.Info
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		DeferCleanup(mockCtrl.Finish)
		mockClient = mock_protoc.NewMockClient(mockCtrl)
		mockSrcStorage = mock_storage.NewMockSource(mockCtrl)
		mockDestStorage = mock_storage.NewMockDestination(mockCtrl)
		tfr = fxfer.NewTransferer(GinkgoLogr, fxfer.WithDisabledRetry())
		srcCommand = sourceCommandFactory(func(cmd *fxfer.SourceCommand) {
			cmd.Storage = mockSrcStorage
			cmd.Client = mockClient
		})
		destCommand = destinationCommandFactory(func(cmd *fxfer.DestinationCommand) {
			cmd.Storage = mockDestStorage
			cmd.Client = mockClient
		})
		callback = func(progress fxfer.Progress) {}
		srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
			i.Path = srcCommand.FilePath
		})
		destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
			i.Path = destCommand.FilePath
		})
	})

	Describe("Transfer", func() {
		It("should create a new destination file if it does not exist", func(ctx context.Context) {
			srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
			})
			destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.Offset = int64(1000)
			})

			gomock.InOrder(
				mockSrcStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					mockClient,
				).Return(srcInfo, nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(xferfile.Info{}, xferfile.ErrFileNotExists),
				mockDestStorage.EXPECT().CreateFile(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					srcInfo.Size,
					srcInfo.ModTime,
					mockClient,
				).Return(nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(destInfo, nil),
			)

			Expect(tfr.Transfer(ctx, srcCommand, destCommand, callback)).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should start over the transfer if the destination modification time is different", func(ctx context.Context) {
			modTime := time.Now()
			srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.ModTime = modTime.Add(time.Minute)
			})
			destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.Offset = int64(700)
				i.ModTime = modTime
			})

			gomock.InOrder(
				mockSrcStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					mockClient,
				).Return(srcInfo, nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(destInfo, nil),
				mockDestStorage.EXPECT().DeleteFile(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(nil),
				mockDestStorage.EXPECT().CreateFile(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					srcInfo.Size,
					srcInfo.ModTime,
					mockClient,
				).Return(nil),
				mockDestStorage.EXPECT().
					GetFileInfo(
						gomock.AssignableToTypeOf(ctx),
						destCommand.FilePath,
						mockClient,
					).
					DoAndReturn(func(ctx context.Context, path string, client protoc.Client) (xferfile.Info, error) {
						destInfo.ModTime = srcInfo.ModTime
						return destInfo, errors.New("error for skipping all other calls, just in test")
					}),
			)

			err := tfr.Transfer(ctx, srcCommand, destCommand, callback)
			Expect(err).To(MatchError("error for skipping all other calls, just in test"))
		}, NodeTimeout(10*time.Second))

		It("should transfer the file from beginning successfully", func(ctx context.Context) {
			modTime := time.Now()
			srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.ModTime = modTime
			})
			destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.Offset = int64(0)
				i.ModTime = modTime
			})

			readCloser := io.NopCloser(strings.NewReader(
				"Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
			))
			defer readCloser.Close()

			gomock.InOrder(
				mockSrcStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					mockClient,
				).Return(srcInfo, nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(xferfile.Info{}, xferfile.ErrFileNotExists),
				mockDestStorage.EXPECT().CreateFile(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					srcInfo.Size,
					srcInfo.ModTime,
					mockClient,
				).Return(nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(destInfo, nil),
				mockSrcStorage.EXPECT().GetFileFromOffset(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					int64(0),
					mockClient,
				).DoAndReturn(func(
					ctx context.Context,
					path string,
					offset int64,
					client protoc.Client,
				) (reader io.ReadCloser, err error) {
					return readCloser, nil
				}),
				mockDestStorage.EXPECT().TransferFileChunk(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					gomock.Any(),
					int64(0),
					mockClient,
				).Return(int64(1000), nil),
				mockDestStorage.EXPECT().FinalizeTransfer(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(nil),
			)

			Expect(tfr.Transfer(ctx, srcCommand, destCommand, callback)).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should transfer the file from the offset successfully", func(ctx context.Context) {
			modTime := time.Now()
			srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.ModTime = modTime
			})
			destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.Offset = int64(700)
				i.ModTime = modTime
			})

			readCloser := io.NopCloser(strings.NewReader(
				"Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
			))
			defer readCloser.Close()

			gomock.InOrder(
				mockSrcStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					mockClient,
				).Return(srcInfo, nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(destInfo, nil),
				mockSrcStorage.EXPECT().GetFileFromOffset(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					int64(700),
					mockClient,
				).DoAndReturn(func(
					ctx context.Context,
					path string,
					offset int64,
					client protoc.Client,
				) (reader io.ReadCloser, err error) {
					return readCloser, nil
				}),
				mockDestStorage.EXPECT().TransferFileChunk(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					gomock.Any(),
					int64(700),
					mockClient,
				).Return(int64(300), nil),
				mockDestStorage.EXPECT().FinalizeTransfer(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(nil),
			)

			Expect(tfr.Transfer(ctx, srcCommand, destCommand, callback)).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should delete file when transfer cannot finish", func(ctx context.Context) {
			modTime := time.Now()
			srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(74)
				i.ModTime = modTime
			})
			destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.Offset = int64(700)
				i.ModTime = modTime
			})

			readCloser := io.NopCloser(strings.NewReader(
				"Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
			))
			defer readCloser.Close()

			gomock.InOrder(
				mockSrcStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					mockClient,
				).Return(srcInfo, nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(destInfo, nil),
				mockSrcStorage.EXPECT().GetFileFromOffset(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					int64(700),
					mockClient,
				).DoAndReturn(func(
					ctx context.Context,
					path string,
					offset int64,
					client protoc.Client,
				) (reader io.ReadCloser, err error) {
					return readCloser, nil
				}),
				mockDestStorage.EXPECT().TransferFileChunk(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					gomock.Any(),
					int64(700),
					mockClient,
				).Do(func(ctx context.Context, path string, reader io.Reader, offset int64, client protoc.Client) {
					buf := new(bytes.Buffer)
					_, err := io.Copy(buf, reader)
					Expect(err).NotTo(HaveOccurred())
				}).Return(int64(300), nil),
				mockDestStorage.EXPECT().FinalizeTransfer(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(storage.ErrFileOrObjectCannotFinalize),
				mockDestStorage.EXPECT().DeleteFile(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(nil),
			)

			Expect(tfr.Transfer(ctx, srcCommand, destCommand, callback)).
				To(MatchError(storage.ErrFileOrObjectCannotFinalize))
		}, NodeTimeout(10*time.Second))
	})

	Context("Transfer with retry", func() {
		BeforeEach(func() {
			tfr = fxfer.NewTransferer(GinkgoLogr, fxfer.WithRetryConfig(fxfer.RetryConfig{
				MaxRetryAttempts: 2,
				InitialDelay:     50 * time.Millisecond,
				MaxDelay:         100 * time.Millisecond,
			}))
		})

		It("should retry the transfer when it fails while chunking", func(ctx context.Context) {
			modTime := time.Now()
			srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.ModTime = modTime
			})
			destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.Offset = int64(700)
				i.ModTime = modTime
			})

			readCloser := io.NopCloser(strings.NewReader(
				"Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
			))
			defer readCloser.Close()

			gomock.InOrder(
				mockSrcStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					mockClient,
				).Return(srcInfo, nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(destInfo, nil),
				mockSrcStorage.EXPECT().GetFileFromOffset(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					int64(700),
					mockClient,
				).DoAndReturn(func(
					ctx context.Context,
					path string,
					offset int64,
					client protoc.Client,
				) (reader io.ReadCloser, err error) {
					return readCloser, nil
				}),
				mockDestStorage.EXPECT().TransferFileChunk(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					gomock.Any(),
					int64(700),
					mockClient,
				).Return(int64(0), gofakeit.Error()),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(xferfile.Info{}, gofakeit.Error()),
			)

			Expect(tfr.Transfer(ctx, srcCommand, destCommand, callback)).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))

		It("should retry the transfer when it fails while finalizing", func(ctx context.Context) {
			modTime := time.Now()
			srcInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.ModTime = modTime
			})
			destInfo = xferfiletest.InfoFactory(func(i *xferfile.Info) {
				i.Size = int64(1000)
				i.Offset = int64(700)
				i.ModTime = modTime
			})

			readCloser := io.NopCloser(strings.NewReader(
				"Lorem Ipsum is simply dummy text of the printing and typesetting industry.",
			))
			defer readCloser.Close()

			gomock.InOrder(
				mockSrcStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					mockClient,
				).Return(srcInfo, nil),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(destInfo, nil),
				mockSrcStorage.EXPECT().GetFileFromOffset(
					gomock.AssignableToTypeOf(ctx),
					srcCommand.FilePath,
					int64(700),
					mockClient,
				).DoAndReturn(func(
					ctx context.Context,
					path string,
					offset int64,
					client protoc.Client,
				) (reader io.ReadCloser, err error) {
					return readCloser, nil
				}),
				mockDestStorage.EXPECT().TransferFileChunk(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					gomock.Any(),
					int64(700),
					mockClient,
				).Return(int64(300), nil),
				mockDestStorage.EXPECT().FinalizeTransfer(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(storage.ErrFileOrObjectCannotFinalize),
				mockDestStorage.EXPECT().GetFileInfo(
					gomock.AssignableToTypeOf(ctx),
					destCommand.FilePath,
					mockClient,
				).Return(xferfile.Info{}, gofakeit.Error()),
			)

			Expect(tfr.Transfer(ctx, srcCommand, destCommand, callback)).To(HaveOccurred())
		}, NodeTimeout(10*time.Second))
	})
})
