package fs

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/wal-g/storages/storage"

	"github.com/stretchr/testify/assert"
)

func TestFSFolder(t *testing.T) {
	tmpDir := setupTmpDir(t)

	defer os.RemoveAll(tmpDir)
	var storageFolder storage.Folder

	storageFolder, err := ConfigureFolder(tmpDir, nil)

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}

func setupTmpDir(t *testing.T) string {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}
	// Create temp directory.
	tmpDir, err := ioutil.TempDir(cwd, "data")
	if err != nil {
		t.Log(err)
	}
	err = os.MkdirAll(tmpDir, 0755)
	if err != nil {
		t.Log(err)
	}
	return tmpDir
}
