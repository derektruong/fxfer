package sftp_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSftp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "sftp suite")
}
