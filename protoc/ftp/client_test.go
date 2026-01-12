package ftp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Client", func() {
	Describe("PoolConfig", func() {
		var cli Client

		Describe("WithMaxPoolSize", func() {
			It("should set the max pool size", func() {
				WithMaxPoolSize(10)(&cli)
				Expect(cli.maxPoolSize).To(Equal(int32(10)))
			})
		})

		Describe("WithMaxConnIdleTime", func() {
			It("should set the max conn idle time", func() {
				WithMaxConnIdleTime(10 * time.Minute)(&cli)
				Expect(cli.maxConnIdleTime).To(Equal(10 * time.Minute))
			})
		})

		Describe("WithMaxConnLifetime", func() {
			It("should set the max conn lifetime", func() {
				WithMaxConnLifetime(10 * time.Minute)(&cli)
				Expect(cli.maxConnLifetime).To(Equal(10 * time.Minute))
			})
		})

		Describe("WithHealthCheckPeriod", func() {
			It("should set the health check period", func() {
				WithHealthCheckPeriod(10 * time.Minute)(&cli)
				Expect(cli.healthCheckPeriod).To(Equal(10 * time.Minute))
			})
		})
	})

	Describe("Client APIs", func() {
		var cli *Client

		BeforeEach(func() {
			cli = NewClient(
				"localhost",
				21,
				"user",
				"pass",
			)
		})

		It("should return correct connection", func() {
			conn := cli.GetConnectionPool(GinkgoLogr)
			Expect(conn).ToNot(BeNil())
		})

		It("should return correct credential", func() {
			cred := cli.GetCredential()
			Expect(cred).To(Equal(*cli))
		})

		It("should return correct connection ID", func() {
			id := cli.GetConnectionID()
			Expect(id).To(Equal("b7aeb335-37a5-5e4b-b3fa-51b91953c59d"))
		})

		It("should return correct address", func() {
			addr := cli.GetAddress()
			Expect(addr).To(Equal("localhost:21"))
		})
	})
})
