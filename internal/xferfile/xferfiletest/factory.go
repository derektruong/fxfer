package xferfiletest

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/derektruong/fxfer/internal/xferfile"
)

func InfoFactory(editFn func(*xferfile.Info)) xferfile.Info {
	dirPath := fmt.Sprintf("%s/%s", gofakeit.Word(), gofakeit.Word())
	fileName := gofakeit.Word()
	ext := gofakeit.FileExtension()
	path := fmt.Sprintf("%s/%s.%s", dirPath, fileName, ext)
	info := &xferfile.Info{
		Path:       path,
		Size:       int64(gofakeit.Number(1, 1000000)),
		Name:       fileName,
		Extension:  ext,
		ModTime:    gofakeit.PastDate(),
		StartTime:  gofakeit.PastDate(),
		FinishTime: gofakeit.FutureDate(),
		Offset:     int64(gofakeit.Number(1, 1000000)),
		Checksum:   gofakeit.ImagePng(10, 10),
		Metadata: map[string]string{
			"multipartKey": strings.Replace(path, filepath.Ext(path), ".part", -1),
		},
	}
	if editFn != nil {
		editFn(info)
	}
	return *info
}
