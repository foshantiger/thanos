package testutil

import (
	"io"
	"os"
	"testing"

	"github.com/pkg/errors"
)

func Copy(t *testing.T, src, dst string) {
	Ok(t, cpy(src, dst))
}

func cpy(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return errors.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}
