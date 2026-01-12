package sftp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/sftp"
)

var mockSFTPServer *MockSFTPServer
var sftpHost string
var sftpRootDir string

var _ = Describe("SFTP Connection pool", Ordered, func() {
	var (
		err  error
		pool *connectionPool
	)

	BeforeAll(func(ctx context.Context) {
		// Create temporary directory for SFTP server
		sftpRootDir = GinkgoT().TempDir()

		// Setup mock SFTP server
		mockSFTPServer, err = NewMockSFTPServer(sftpRootDir, "test", "test")
		Expect(err).ToNot(HaveOccurred())
		mockSFTPServer.Start()
		sftpHost = mockSFTPServer.Addr()

		DeferCleanup(func() {
			if mockSFTPServer != nil {
				mockSFTPServer.Stop()
			}
		})

		pool = newConnectionPool(
			GinkgoLogr,
			defaultMaxPoolSize, defaultMaxConnIdleTime, defaultMaxConnLifetime, defaultHealthCheckPeriod,
		)
		DeferCleanup(pool.Close)
	}, NodeTimeout(2*time.Minute))

	Describe("InitializeIdleConnection", func() {
		It("should cache the connection if the connection is not cached", func(ctx context.Context) {
			Expect(pool.InitializeIdleConnection(Client{
				Host:     sftpHost,
				Username: mockSFTPServer.Username(),
				Password: mockSFTPServer.Password(),
			})).To(Succeed())
			Expect(pool.connPool).ToNot(BeNil())
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
			sftpClient := conn.sftpClient
			Expect(sftpClient).ToNot(BeNil())

			filePath := mockSFTPServer.TestDir() + "/test-sftp-file.txt"
			file, err := sftpClient.Create(filePath)
			Expect(err).ToNot(HaveOccurred())
			defer file.Close()
			_, err = file.Write([]byte("Lorem ipsum dolor sit amet"))
			Expect(err).ToNot(HaveOccurred())

			By("Get the file size")
			size, _, err := pool.GetFileSizeAndModTime(ctx, filePath)
			Expect(err).ToNot(HaveOccurred())
			Expect(size).To(Equal(int64(26)))
		}, NodeTimeout(30*time.Second))
	})

	Describe("RetrieveFromOffset", func() {
		var sftpClient *sftp.Client
		var content, filePath string

		BeforeAll(func() {
			Expect(pool.connPool).ToNot(BeNil())
			content = "Lorem Ipsum is simply dummy text of the printing and typesetting industry."

			By("acquire the connection")
			resource, err := pool.connPool.Acquire(pool.doneCtx)
			Expect(err).ToNot(HaveOccurred())
			defer resource.Release()

			By("store the file")
			conn := resource.Value()
			sftpClient = conn.sftpClient
			filePath = mockSFTPServer.TestDir() + "/test-sftp-file.txt"
			file, err := sftpClient.Create(filePath)
			Expect(err).ToNot(HaveOccurred())
			DeferCleanup(file.Close)
			_, err = file.Write([]byte(content))
			Expect(err).ToNot(HaveOccurred())
		})

		It("should retrieve the whole file from the offset 0", func(ctx context.Context) {
			buf := new(strings.Builder)
			reader, err := pool.RetrieveFileFromOffset(ctx, filePath, 0)
			Expect(err).ToNot(HaveOccurred())
			Expect(reader).ToNot(BeNil())
			defer reader.Close()
			_, err = io.Copy(buf, reader)
			Expect(err).ToNot(HaveOccurred())
			Expect(buf.String()).To(Equal(content))
		}, NodeTimeout(30*time.Second))

		It("should retrieve the file content from the offset 10", func(ctx context.Context) {
			buf := new(strings.Builder)
			reader, err := pool.RetrieveFileFromOffset(ctx, filePath, 10)
			Expect(err).ToNot(HaveOccurred())
			Expect(reader).ToNot(BeNil())
			defer reader.Close()
			_, err = io.Copy(buf, reader)
			Expect(err).ToNot(HaveOccurred())
			Expect(buf.String()).To(Equal(content[10:]))
		}, NodeTimeout(30*time.Second))
	})

	Describe("MakeDirectoryAll", func() {
		It("should create the directory", func(ctx context.Context) {
			Expect(pool.connPool).ToNot(BeNil())
			resource, err := pool.connPool.Acquire(pool.doneCtx)
			Expect(err).ToNot(HaveOccurred())
			defer resource.Release()

			By("create the directory")
			expectDir := fmt.Sprintf("%s/prefix1/prefix2/prefix3", mockSFTPServer.TestDir())
			Expect(pool.MakeDirectoryAll(ctx, expectDir)).To(Succeed())

			By("assert the directory is created")
			conn := resource.Value()
			info, err := conn.sftpClient.Stat(expectDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(info).ToNot(BeNil())
			Expect(info.IsDir()).To(BeTrue())
		}, NodeTimeout(30*time.Second))
	})

	Describe("CreateOrOverwriteFile", func() {
		It("should create the file", func(ctx context.Context) {
			By("acquire the connection")
			Expect(pool.connPool).ToNot(BeNil())
			resource, err := pool.connPool.Acquire(pool.doneCtx)
			Expect(err).ToNot(HaveOccurred())
			defer resource.Release()
			totalResources := pool.connPool.Stat().AcquiredResources()
			Expect(totalResources).To(Equal(int32(1)))

			By("store the file")
			fileName := fmt.Sprintf("%s/%s.txt", mockSFTPServer.TestDir(), gofakeit.Name())
			reader := strings.NewReader(string(bytes.Repeat([]byte("a"), 74)))
			Expect(pool.CreateOrOverwriteFile(ctx, fileName, reader)).To(Succeed())

			By("assert the file size")
			size, _, err := pool.GetFileSizeAndModTime(ctx, fileName)
			Expect(err).ToNot(HaveOccurred())
			Expect(size).To(Equal(int64(74)))
		}, NodeTimeout(30*time.Second))
	})

	Describe("AppendToFile", func() {
		It("should append the file", func(ctx context.Context) {
			By("acquire the connection")
			Expect(pool.connPool).ToNot(BeNil())
			resource, err := pool.connPool.Acquire(pool.doneCtx)
			Expect(err).ToNot(HaveOccurred())
			defer resource.Release()
			totalResources := pool.connPool.Stat().AcquiredResources()
			Expect(totalResources).To(Equal(int32(1)))

			By("store the file")
			fileName := fmt.Sprintf("%s/%s.txt", mockSFTPServer.TestDir(), gofakeit.Name())
			reader := strings.NewReader(string(bytes.Repeat([]byte("a"), 74)))
			Expect(pool.CreateOrOverwriteFile(ctx, fileName, reader)).To(Succeed())

			By("append the file")
			reader = strings.NewReader(string(bytes.Repeat([]byte("b"), 74)))
			Expect(pool.AppendToFile(ctx, fileName, reader, 74)).To(Succeed())

			By("assert the file size")
			size, _, err := pool.GetFileSizeAndModTime(ctx, fileName)
			Expect(err).ToNot(HaveOccurred())
			Expect(size).To(Equal(int64(148)))
		}, NodeTimeout(30*time.Second))
	})

	// put this test container at last because it will terminate all connections
	Context("Verify the FTP connection pooling", Ordered, func() {
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
				Host:     sftpHost,
				Username: mockSFTPServer.Username(),
				Password: mockSFTPServer.Password(),
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
				Host:     sftpHost,
				Username: mockSFTPServer.Username(),
				Password: mockSFTPServer.Password(),
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
		}, NodeTimeout(30*time.Second))
	})
})
