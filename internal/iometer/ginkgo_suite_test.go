package iometer_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "iometer tests suite")
}
