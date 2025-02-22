package sliceutils

import "strings"

// Contains checks if a string is in a slice, it is case-insensitive
func Contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, item) {
			return true
		}
	}
	return false
}
