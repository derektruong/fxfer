package xferfiletest

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/derektruong/fxfer/internal/xferfile"
)

func InfoFactory() *xferfile.Info {
	ext := gofakeit.FileExtension()
	path := fmt.Sprintf("%s/%s.%s", gofakeit.Word(), gofakeit.Word(), ext)
	return &xferfile.Info{
		Path:       path,
		Size:       gofakeit.Int64(),
		Name:       gofakeit.Word(),
		Extension:  ext,
		ModTime:    gofakeit.PastDate(),
		StartTime:  gofakeit.PastDate(),
		FinishTime: gofakeit.FutureDate(),
		Offset:     int64(gofakeit.Number(1, 1000000)),
		Checksum:   gofakeit.ImagePng(10, 10),
		Metadata: map[string]string{
			"multipartKey": strings.Replace(path, ext, ".part", -1),
		},
	}
}
