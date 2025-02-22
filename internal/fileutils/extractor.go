package fileutils

import (
	"errors"
	"path/filepath"
	"strings"
)

// ExtractFileParts extracts the prefix, file name, and extension from the path
// in the format <prefix_path>/<file_name>.<ext>
func ExtractFileParts(filePath string) (prefix, fileName, fileExt string, err error) {
	if filePath == "" {
		err = errors.New("asset file parts are required")
		return
	}
	if fileExt = filepath.Ext(filePath); fileExt == "" {
		err = errors.New("file extension is required")
		return
	}
	fileExt = strings.TrimPrefix(fileExt, ".")
	dir := filepath.Dir(filePath)
	base := filepath.Base(filePath)
	fileName = strings.TrimSuffix(base, filepath.Ext(base))
	if dir == "." || dir == "" {
		prefix = ""
	} else {
		prefix = dir
	}
	return
}
