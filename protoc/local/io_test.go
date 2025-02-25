package local

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Local IO APIs", func() {
	var io *IO

	BeforeEach(func() {
		io = NewIO()
	})

	It("should return error for GetConnectionPool", func() {
		Expect(func ()  {
			io.GetConnectionPool(GinkgoLogr)
		}).Should(PanicWith(MatchError(errors.ErrUnsupported)))
	})

	It("should return error for GetS3API", func() {
		Expect(func ()  {
			io.GetS3API()
		}).Should(PanicWith(MatchError(errors.ErrUnsupported)))
	})

	It("should return correct io credential", func() {
		Expect(io.GetCredential()).To(Equal(*io))
	})

	It("should return correct connection ID", func() {
		Expect(io.GetConnectionID()).To(Equal(""))
	})
})
