package protocutils_test

import (
	"github.com/derektruong/fxfer/internal/protocutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Builder", func() {
	Describe("BuildAddress", func() {
		It("should return the host if the port is not provided", func() {
			address := protocutils.BuildAddress("localhost", 0)
			Expect(address).To(Equal("localhost"))
		})

		It("should return the host and port if the port is provided", func() {
			address := protocutils.BuildAddress("localhost:8080", 8080)
			Expect(address).To(Equal("localhost:8080"))
		})

		It("should return an empty string if the host is empty", func() {
			address := protocutils.BuildAddress("", 8080)
			Expect(address).To(Equal(""))
		})
	})
})
