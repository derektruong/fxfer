package s3

import (
	. "github.com/onsi/ginkgo/v2"
)

// destStorageFactory creates a new Destination store with the given storeEditorFn applied.
func destStorageFactory(storageEditorFn func(d *Destination)) *Destination {
	GinkgoHelper()
	store := NewDestination(GinkgoLogr)
	if storageEditorFn != nil {
		storageEditorFn(store)
	}
	return store
}
