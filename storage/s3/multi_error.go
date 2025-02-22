package s3

import (
	"errors"
)

func newMultiError(errs []error) error {
	message := "Multiple errors occurred:\n"
	joinedErrors := errors.Join(errs...)
	if joinedErrors == nil {
		return nil
	}
	return errors.New(message + joinedErrors.Error())
}
