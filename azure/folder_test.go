package azure

import (
	"testing"

	"github.com/wal-g/storages/storage"

	"github.com/stretchr/testify/assert"
)

func TestAzureFolder(t *testing.T) {
	t.Skip("Credentials needed to run Azure Storage tests")

	storageFolder, err := ConfigureFolder("azure://test-container/test-folder/Sub0",
		make(map[string]string))

	assert.NoError(t, err)

	storage.RunFolderTest(storageFolder, t)
}
