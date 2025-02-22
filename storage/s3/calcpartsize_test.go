package s3

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

const enableTestDebugOutput = false

var _ = Describe("Calculate Part Size", func() {
	var (
		mockCtrl *gomock.Controller
		store    *Destination
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		DeferCleanup(mockCtrl.Finish)
		store = destStorageFactory(nil)
	})

	It("should calculate optimal part size", func() {
		// sanity check
		HighestApplicablePartSize := store.MaxObjectSize / store.MaxMultipartParts
		if store.MaxObjectSize%store.MaxMultipartParts > 0 {
			HighestApplicablePartSize++
		}
		RemainderWithHighestApplicablePartSize := store.MaxObjectSize % HighestApplicablePartSize

		// some of these tests are actually duplicates, as they specify the same size
		// in bytes - two ways to describe the same thing. That is wanted, in order
		// to provide a full picture from any angle.
		testcases := []int64{
			0,
			1,

			store.PreferredPartSize - 1,
			store.PreferredPartSize,
			store.PreferredPartSize + 1,

			store.MinPartSize - 1,
			store.MinPartSize,
			store.MinPartSize + 1,

			store.MinPartSize*(store.MaxMultipartParts-1) - 1,
			store.MinPartSize * (store.MaxMultipartParts - 1),
			store.MinPartSize*(store.MaxMultipartParts-1) + 1,

			store.MinPartSize*store.MaxMultipartParts - 1,
			store.MinPartSize * store.MaxMultipartParts,
			store.MinPartSize*store.MaxMultipartParts + 1,

			store.MinPartSize*(store.MaxMultipartParts+1) - 1,
			store.MinPartSize * (store.MaxMultipartParts + 1),
			store.MinPartSize*(store.MaxMultipartParts+1) + 1,

			(HighestApplicablePartSize-1)*store.MaxMultipartParts - 1,
			(HighestApplicablePartSize - 1) * store.MaxMultipartParts,
			(HighestApplicablePartSize-1)*store.MaxMultipartParts + 1,

			HighestApplicablePartSize*(store.MaxMultipartParts-1) - 1,
			HighestApplicablePartSize * (store.MaxMultipartParts - 1),
			HighestApplicablePartSize*(store.MaxMultipartParts-1) + 1,

			HighestApplicablePartSize*(store.MaxMultipartParts-1) + RemainderWithHighestApplicablePartSize - 1,
			HighestApplicablePartSize*(store.MaxMultipartParts-1) + RemainderWithHighestApplicablePartSize,
			HighestApplicablePartSize*(store.MaxMultipartParts-1) + RemainderWithHighestApplicablePartSize + 1,

			store.MaxObjectSize - 1,
			store.MaxObjectSize,
			store.MaxObjectSize + 1,

			(store.MaxObjectSize/store.MaxMultipartParts)*(store.MaxMultipartParts-1) - 1,
			(store.MaxObjectSize / store.MaxMultipartParts) * (store.MaxMultipartParts - 1),
			(store.MaxObjectSize/store.MaxMultipartParts)*(store.MaxMultipartParts-1) + 1,

			store.MaxPartSize*(store.MaxMultipartParts-1) - 1,
			store.MaxPartSize * (store.MaxMultipartParts - 1),
			store.MaxPartSize*(store.MaxMultipartParts-1) + 1,

			store.MaxPartSize*store.MaxMultipartParts - 1,
			store.MaxPartSize * store.MaxMultipartParts,
			// we cannot calculate a part size for store.MaxPartSize*store.MaxMultipartParts + 1.
			// This case is tested in TestCalcOptimalPartSize_ExceedingMaxPartSize.
		}

		for _, size := range testcases {
			assertCalculatedPartSize(store, size)
		}

		if enableTestDebugOutput {
			_, _ = fmt.Fprintf(GinkgoWriter, "HighestApplicablePartSize %v\n", HighestApplicablePartSize)
			_, _ = fmt.Fprintf(
				GinkgoWriter,
				"RemainderWithHighestApplicablePartSize %v\n", RemainderWithHighestApplicablePartSize,
			)
		}
	})

	It("calculate optimal part size with all upload sizes", func() {
		store = destStorageFactory(func(s *Destination) {
			s.MinPartSize = 5
			s.MaxPartSize = 5 * 1024
			s.PreferredPartSize = 10
			s.MaxMultipartParts = 1000
			s.MaxObjectSize = store.MaxPartSize * store.MaxMultipartParts
		})

		// sanity check
		if store.MaxObjectSize > store.MaxPartSize*store.MaxMultipartParts {
			_, _ = fmt.Fprintf(
				GinkgoWriter,
				"MaxObjectSize %v can never be achieved, as MaxMultipartParts %v and MaxPartSize %v only allow for an upload of %v bytes total.\n",
				store.MaxObjectSize, store.MaxMultipartParts, store.MaxPartSize, store.MaxMultipartParts*store.MaxPartSize,
			)
			return
		}

		for size := int64(0); size <= store.MaxObjectSize; size++ {
			assertCalculatedPartSize(store, size)
		}
	})

	It("calculate optimal part size when exceeding max part size", func() {
		size := store.MaxPartSize*store.MaxMultipartParts + 1

		optimalPartSize, err := store.calcOptimalPartSize(size)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).
			To(Equal(fmt.Sprintf(
				"calcOptimalPartSize: to upload %v bytes optimalPartSize %v must exceed MaxPartSize %v",
				size, optimalPartSize, store.MaxPartSize,
			)))
	})
})

func assertCalculatedPartSize(store *Destination, size int64) {
	optimalPartSize, err := store.calcOptimalPartSize(size)
	Expect(err).To(BeNil(), fmt.Sprintf("Size %d, no error should be returned.\n", size))

	// number of parts with the same size
	equalParts := size / optimalPartSize
	// size of the last part (or 0 if no spare part is needed)
	lastPartSize := size % optimalPartSize

	prelude := fmt.Sprintf("Size %d, %d parts of size %d, lastpart %d: ", size, equalParts, optimalPartSize, lastPartSize)

	Expect(optimalPartSize < store.MinPartSize).
		To(BeFalse(), fmt.Sprintf(prelude+"optimalPartSize < MinPartSize %d.\n", store.MinPartSize))
	Expect(optimalPartSize > store.MaxPartSize).
		To(BeFalse(), fmt.Sprintf(prelude+"optimalPartSize > MaxPartSize %d.\n", store.MaxPartSize))
	Expect(lastPartSize == 0 && equalParts > store.MaxMultipartParts).
		To(BeFalse(), fmt.Sprintf(prelude+"more parts than MaxMultipartParts %d.\n", store.MaxMultipartParts))
	Expect(lastPartSize > 0 && equalParts > store.MaxMultipartParts-1).
		To(BeFalse(), fmt.Sprintf(prelude+"more parts than MaxMultipartParts %d.\n", store.MaxMultipartParts))
	Expect(lastPartSize > store.MaxPartSize).
		To(BeFalse(), fmt.Sprintf(prelude+"lastpart > MaxPartSize %d.\n", store.MaxPartSize))
	Expect(lastPartSize > optimalPartSize).
		To(BeFalse(), fmt.Sprintf(prelude+"lastpart > optimalPartSize %d.\n", optimalPartSize))
	Expect(size <= optimalPartSize*store.MaxMultipartParts).
		To(BeTrue(), fmt.Sprintf(prelude+"upload does not fit in %d parts.\n", store.MaxMultipartParts))

	if enableTestDebugOutput {
		_, _ = fmt.Fprintf(GinkgoWriter, prelude+"does exceed MaxObjectSize: %t.\n", size > store.MaxObjectSize)
	}
}
