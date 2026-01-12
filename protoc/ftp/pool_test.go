package ftp

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	"github.com/derektruong/fxfer/internal/xferfile"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/brianvoe/gofakeit/v7"
)

var mockFTPServer *MockFTPServer
var ftpHost string
var ftpRootDir string

var _ = Describe("FTP Connection pool", func() {
	var (
		err  error
		pool *connectionPool
	)

	Context("Using connection pool", Ordered, func() {
		BeforeAll(func(ctx context.Context) {
			// Create temporary directory for FTP server
			ftpRootDir = GinkgoT().TempDir()

			// Setup mock FTP server
			mockFTPServer, err = NewMockFTPServer(ftpRootDir)
			Expect(err).ToNot(HaveOccurred())
			mockFTPServer.Start()
			ftpHost = mockFTPServer.Addr()

			DeferCleanup(func() {
				if mockFTPServer != nil {
					mockFTPServer.Stop()
				}
			})

			pool = newConnectionPool(
				GinkgoLogr,
				defaultMaxPoolSize,
				defaultMaxConnIdleTime,
				defaultMaxConnLifetime,
				defaultHealthCheckPeriod,
			)
			DeferCleanup(pool.Close)
		}, NodeTimeout(2*time.Minute))

		Describe("InitializeIdleConnection", func() {
			It("should acquire the connection if the connection is not cached", func(ctx context.Context) {
				Expect(pool.InitializeIdleConnection(Client{
					Host:     ftpHost,
					Username: "test",
					Password: "test",
				})).To(Succeed())
			}, NodeTimeout(30*time.Second))
		})

		Describe("GetFileSizeAndModTime", func() {
			It("should return the file size", func(ctx context.Context) {
				By("acquire the connection")
				Expect(pool.connPool).ToNot(BeNil())
				resource, err := pool.connPool.Acquire(pool.doneCtx)
				Expect(err).ToNot(HaveOccurred())
				defer resource.Release()
				totalResources := pool.connPool.Stat().AcquiredResources()
				Expect(totalResources).To(Equal(int32(1)))

				By("store the file")
				conn := resource.Value()
				Expect(conn.serverConn.Stor(
					"test.txt",
					strings.NewReader("Lorem Ipsum is simply dummy text of the printing and typesetting industry."),
				)).To(Succeed())

				By("assert the file size")
				size, _, err := pool.GetFileSizeAndModTime(ctx, "test.txt")
				Expect(err).ToNot(HaveOccurred())
				Expect(size).To(Equal(int64(74)))
			}, NodeTimeout(30*time.Second))

			It("should return error file not exists if get 550 error", func(ctx context.Context) {
				_, _, err := pool.GetFileSizeAndModTime(ctx, "not-exists.txt")
				Expect(err).To(MatchError(xferfile.ErrFileNotExists))
			})
		})

		Describe("RetrieveFileFromOffset", func() {
			var content string

			BeforeAll(func() {
				Expect(pool.connPool).ToNot(BeNil())
				content = "Lorem Ipsum is simply dummy text of the printing and typesetting industry."

				By("acquire the connection")
				resource, err := pool.connPool.Acquire(pool.doneCtx)
				Expect(err).ToNot(HaveOccurred())
				defer resource.Release()
				totalResources := pool.connPool.Stat().AcquiredResources()
				Expect(totalResources).To(Equal(int32(1)))

				By("store the file")
				conn := resource.Value()
				Expect(conn.serverConn.Stor("test.txt", strings.NewReader(content))).To(Succeed())
			})

			It("should retrieve the whole file content from offset 0", func(ctx context.Context) {
				buf := new(strings.Builder)
				reader, err := pool.RetrieveFileFromOffset(ctx, "test.txt", 0)
				Expect(err).ToNot(HaveOccurred())
				Expect(reader).ToNot(BeNil())
				defer reader.Close()
				_, err = io.Copy(buf, reader)
				Expect(err).ToNot(HaveOccurred())
				Expect(buf.String()).To(Equal(content))
			}, NodeTimeout(30*time.Second))

			It("should retrieve the file content from offset 10", func(ctx context.Context) {
				buf := new(strings.Builder)
				reader, err := pool.RetrieveFileFromOffset(ctx, "test.txt", 10)
				Expect(err).ToNot(HaveOccurred())
				Expect(reader).ToNot(BeNil())
				defer reader.Close()
				_, err = io.Copy(buf, reader)
				Expect(err).ToNot(HaveOccurred())
				Expect(buf.String()).To(Equal(content[10:]))
			}, NodeTimeout(30*time.Second))

			It("should return error file not exists if get 550 error", func(ctx context.Context) {
				_, err := pool.RetrieveFileFromOffset(ctx, "not-exists.txt", 0)
				Expect(err).To(MatchError(xferfile.ErrFileNotExists))
			})
		})

		Describe("MakeDirectoryAll", func() {
			It("should create the directory", func(ctx context.Context) {
				Expect(pool.connPool).ToNot(BeNil())
				resource, err := pool.connPool.Acquire(pool.doneCtx)
				Expect(err).ToNot(HaveOccurred())
				defer resource.Release()

				By("create the directory")
				Expect(pool.MakeDirectoryAll(ctx, "prefix1/prefix2/prefix3")).To(Succeed())

				By("assert the directory is created")
				conn := resource.Value()
				Expect(conn.serverConn.ChangeDir("prefix1/prefix2/prefix3")).To(Succeed())
			}, NodeTimeout(30*time.Second))
		})

		Describe("CreateOrOverwriteFile", func() {
			It("should store the file", func(ctx context.Context) {
				By("acquire the connection")
				Expect(pool.connPool).ToNot(BeNil())
				resource, err := pool.connPool.Acquire(pool.doneCtx)
				Expect(err).ToNot(HaveOccurred())
				defer resource.Release()
				totalResources := pool.connPool.Stat().AcquiredResources()
				Expect(totalResources).To(Equal(int32(1)))

				By("store the file")
				fileName := gofakeit.Name() + ".txt"
				reader := strings.NewReader(string(bytes.Repeat([]byte("a"), 74)))
				Expect(pool.CreateOrOverwriteFile(ctx, fileName, reader)).To(Succeed())

				By("assert the file size")
				size, _, err := pool.GetFileSizeAndModTime(ctx, fileName)
				Expect(err).ToNot(HaveOccurred())
				Expect(size).To(Equal(int64(74)))
			}, NodeTimeout(30*time.Second))
		})

		Describe("AppendToFile", func() {
			It("should append the content to the file", func(ctx context.Context) {
				By("acquire the connection")
				Expect(pool.connPool).ToNot(BeNil())
				resource, err := pool.connPool.Acquire(pool.doneCtx)
				Expect(err).ToNot(HaveOccurred())
				defer resource.Release()
				totalResources := pool.connPool.Stat().AcquiredResources()
				Expect(totalResources).To(Equal(int32(1)))

				By("append the content to the file")
				fileName := gofakeit.Name() + ".txt"
				reader := strings.NewReader(string(bytes.Repeat([]byte("a"), 74)))
				Expect(pool.CreateOrOverwriteFile(ctx, fileName, reader)).To(Succeed())

				reader = strings.NewReader(string(bytes.Repeat([]byte("b"), 74)))
				Expect(pool.AppendToFile(ctx, fileName, reader, 74)).To(Succeed())

				By("assert the file size")
				size, _, err := pool.GetFileSizeAndModTime(ctx, fileName)
				Expect(err).ToNot(HaveOccurred())
				Expect(size).To(Equal(int64(148)))
			}, NodeTimeout(30*time.Second))
		})
	})

	// put this test at last because it will terminate all connections
	Context("Verify the FTP connection pooling", Ordered, func() {
		BeforeAll(func(ctx context.Context) {
			// Create temporary directory for FTP server
			ftpRootDir = GinkgoT().TempDir()

			// Setup mock FTP server
			mockFTPServer, err = NewMockFTPServer(ftpRootDir)
			Expect(err).ToNot(HaveOccurred())
			mockFTPServer.Start()
			ftpHost = mockFTPServer.Addr()

			DeferCleanup(func() {
				if mockFTPServer != nil {
					mockFTPServer.Stop()
				}
			})
		}, NodeTimeout(30*time.Second))

		It("should release the connection after the idle time", func(ctx context.Context) {
			pool = newConnectionPool(
				GinkgoLogr,
				defaultMaxPoolSize,
				100*time.Millisecond,
				defaultMaxConnLifetime,
				50*time.Millisecond,
			)
			defer pool.Close()

			By("create the connection pool")
			Expect(pool.InitializeIdleConnection(Client{
				Host:     ftpHost,
				Username: "test",
				Password: "test",
			})).To(Succeed())

			By("acquire the connection")
			resource, err := pool.connPool.Acquire(pool.doneCtx)
			Expect(err).ToNot(HaveOccurred())
			totalResources := pool.connPool.Stat().TotalResources()
			Expect(totalResources).To(Equal(int32(1)))
			resource.Release()

			By("assert the connection is released")
			Eventually(func(g Gomega) {
				totalResources = pool.connPool.Stat().TotalResources()
				g.Expect(totalResources).To(Equal(int32(0)))
			}).WithContext(ctx).Should(Succeed())
		}, NodeTimeout(30*time.Second))

		It("should release the connection after the lifetime", func(ctx context.Context) {
			pool = newConnectionPool(
				GinkgoLogr,
				defaultMaxPoolSize,
				defaultMaxConnIdleTime,
				300*time.Millisecond,
				50*time.Millisecond,
			)
			defer pool.Close()

			By("create the connection pool")
			Expect(pool.InitializeIdleConnection(Client{
				Host:     ftpHost,
				Username: "test",
				Password: "test",
			})).To(Succeed())

			Expect(pool.connPool).ToNot(BeNil())

			By("acquire the connection")
			resource, err := pool.connPool.Acquire(pool.doneCtx)
			Expect(err).ToNot(HaveOccurred())
			totalResources := pool.connPool.Stat().TotalResources()
			Expect(totalResources).To(Equal(int32(1)))
			resource.Release()

			By("assert the connection is released")
			Eventually(func(g Gomega) {
				totalResources = pool.connPool.Stat().TotalResources()
				g.Expect(totalResources).To(Equal(int32(0)))
			}).WithContext(ctx).Should(Succeed())
		})
	})
})
