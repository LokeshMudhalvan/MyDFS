package storage

import (
	"testing"
)

func TestPathTransformFunc(t *testing.T) {
	fs := NewFileStorage(HashPathTransform, 5)
	fp, err := fs.getFilePath("Mothersbestimages")
	if err != nil {
		t.Error("Failed: could not convert key to file path:", err)
	}

	t.Logf("This is the generated file path: %s/%s", fp.basePath, fp.fileName)
}
