package s3

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client APIs", func() {
	var cli *Client

	BeforeEach(func() {
		cli = NewClient(
			"https://local-s3.com",
			"test-bucket",
			"us-east-1",
			"123123124234",
			"36456457457",
		)
	})

	It("should return error for GetConnectionPool", func() {
		Expect(func ()  {
			cli.GetConnectionPool(GinkgoLogr)
		}).Should(PanicWith(MatchError(errors.ErrUnsupported)))
	})

	It("should return correct connection", func() {
		s3API := cli.GetS3API()
		Expect(s3API).ToNot(BeNil())
	})

	It("should return correct credential", func() {
		cred := cli.GetCredential()
		Expect(cred).To(Equal(*cli))
	})

	It("should return correct connection ID", func() {
		id := cli.GetConnectionID()
		Expect(id).To(Equal("29355f94-1a9a-5325-9e6c-52a7ace57de3"))
	})

	It("should return correct URI", func() {
		url := cli.GetURI()
		Expect(url).To(Equal("local-s3.com/test-bucket"))
	})
})
