package gcs

import (
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/storages/storage"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	ContextTimeout        = "GCS_CONTEXT_TIMEOUT"
	NormalizePrefix       = "GCS_NORMALIZE_PREFIX"
	defaultContextTimeout = 60 * 60 // 1 hour
	maxRetryDelay         = 5 * time.Minute
)

var (
	// MaxRetries limits upload and download retries during interaction with GCS.
	MaxRetries = 16

	// BaseRetryDelay defines the first delay for retry.
	BaseRetryDelay = 128 * time.Millisecond

	// SettingList provides a list of GCS folder settings.
	SettingList = []string{
		ContextTimeout,
		NormalizePrefix,
	}
)

func NewError(err error, format string, args ...interface{}) storage.Error {
	return storage.NewError(err, "GCS", format, args...)
}

func NewFolder(bucket *gcs.BucketHandle, path string, contextTimeout int, normalizePrefix bool) *Folder {
	return &Folder{bucket, path, contextTimeout, normalizePrefix}
}

func ConfigureFolder(prefix string, settings map[string]string) (storage.Folder, error) {
	normalizePrefix := true

	if normalizePrefixStr, ok := settings[NormalizePrefix]; ok {
		var err error
		normalizePrefix, err = strconv.ParseBool(normalizePrefixStr)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse %s", NormalizePrefix)
		}
	}

	ctx := context.Background()

	client, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, NewError(err, "Unable to create client")
	}

	var bucketName, path string
	if normalizePrefix {
		bucketName, path, err = storage.GetPathFromPrefix(prefix)
	} else {
		// Special mode for WAL-E compatibility with strange prefixes
		bucketName, path, err = storage.ParsePrefixAsURL(prefix)
		if err == nil && path[0] == '/' {
			path = path[1:]
		}
	}

	if err != nil {
		return nil, NewError(err, "Unable to parse prefix %v", prefix)
	}

	bucket := client.Bucket(bucketName)

	path = storage.AddDelimiterToPath(path)

	contextTimeout := defaultContextTimeout
	if contextTimeoutStr, ok := settings[ContextTimeout]; ok {
		contextTimeout, err = strconv.Atoi(contextTimeoutStr)
		if err != nil {
			return nil, NewError(err, "Unable to parse Context Timeout %v", prefix)
		}
	}
	return NewFolder(bucket, path, contextTimeout, normalizePrefix), nil
}

// Folder represents folder in GCP
type Folder struct {
	bucket          *gcs.BucketHandle
	path            string
	contextTimeout  int
	normalizePrefix bool
}

func (folder *Folder) GetPath() string {
	return folder.path
}

func (folder *Folder) ListFolder() (objects []storage.Object, subFolders []storage.Folder, err error) {
	prefix := storage.AddDelimiterToPath(folder.path)
	ctx, cancel := folder.createTimeoutContext()
	defer cancel()
	it := folder.bucket.Objects(ctx, &gcs.Query{Delimiter: "/", Prefix: prefix})
	for {
		objAttrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, NewError(err, "Unable to iterate %v", folder.path)
		}
		if objAttrs.Prefix != "" {

			if objAttrs.Prefix != prefix+"/" {
				// Sometimes GCS returns "//" folder - skip it
				subFolders = append(subFolders, NewFolder(folder.bucket, objAttrs.Prefix, folder.contextTimeout, folder.normalizePrefix))
			}
		} else {
			objName := strings.TrimPrefix(objAttrs.Name, prefix)
			if objName != "" {
				// GCS returns the current directory - skip it.
				objects = append(objects, storage.NewLocalObject(objName, objAttrs.Updated, objAttrs.Size))
			}
		}
	}
	return
}

func (folder *Folder) createTimeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*time.Duration(folder.contextTimeout))
}

func (folder *Folder) DeleteObjects(objectRelativePaths []string) error {
	for _, objectRelativePath := range objectRelativePaths {
		path := folder.joinPath(folder.path, objectRelativePath)
		object := folder.bucket.Object(path)
		tracelog.DebugLogger.Printf("Delete %v\n", path)
		ctx, cancel := folder.createTimeoutContext()
		defer cancel()
		err := object.Delete(ctx)
		if err != nil && err != gcs.ErrObjectNotExist {
			return NewError(err, "Unable to delete object %v", path)
		}
	}
	return nil
}

func (folder *Folder) Exists(objectRelativePath string) (bool, error) {
	path := folder.joinPath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	ctx, cancel := folder.createTimeoutContext()
	defer cancel()
	_, err := object.Attrs(ctx)
	if err == gcs.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, NewError(err, "Unable to stat object %v", path)
	}
	return true, nil
}

func (folder *Folder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewFolder(folder.bucket, folder.joinPath(folder.path, subFolderRelativePath), folder.contextTimeout, folder.normalizePrefix)
}

func (folder *Folder) ReadObject(objectRelativePath string) (io.ReadCloser, error) {
	path := folder.joinPath(folder.path, objectRelativePath)
	object := folder.bucket.Object(path)
	reader, err := object.NewReader(context.Background())
	if err == gcs.ErrObjectNotExist {
		return nil, storage.NewObjectNotFoundError(path)
	}
	return ioutil.NopCloser(reader), err
}

func (folder *Folder) PutObject(name string, content io.Reader) error {
	tracelog.DebugLogger.Printf("Put %v into %v\n", name, folder.path)
	object := folder.bucket.Object(folder.joinPath(folder.path, name))

	ctx, cancel := folder.createTimeoutContext()
	defer cancel()

	chunkNum := 0
	tmpChunks := make([]*gcs.ObjectHandle, 0)

	for {
		tmpChunkName := folder.joinPath(name+"_chunks", "chunk"+strconv.Itoa(chunkNum))
		objectChunk := folder.bucket.Object(folder.joinPath(folder.path, tmpChunkName))
		chunkUploader := NewUploader(objectChunk)
		dataChunk := chunkUploader.allocateBuffer()

		n, err := fillBuffer(content, dataChunk)
		if err != nil && err != io.EOF {
			tracelog.ErrorLogger.Printf("Unable to read content of %s, err: %v", name, err)
			return NewError(err, "Unable to read a chunk of data to upload")
		}

		if n == 0 {
			break
		}

		chunk := chunk{
			name:  tmpChunkName,
			index: chunkNum,
			data:  dataChunk,
			size:  n,
		}

		if err := chunkUploader.UploadChunk(ctx, chunk); err != nil {
			return NewError(err, "Unable to upload an object chunk")
		}

		tmpChunks = append(tmpChunks, objectChunk)

		chunkNum++

		if err == io.EOF {
			break
		}
	}

	tracelog.DebugLogger.Printf("Compose file %v from chunks\n", object.ObjectName())

	uploader := NewUploader(object)
	if err := uploader.ComposeObject(ctx, tmpChunks); err != nil {
		return NewError(err, "Unable to compose object")
	}

	tracelog.DebugLogger.Printf("Remove temporary chunks for %v\n", object.ObjectName())

	if err := uploader.CleanUpChunks(ctx, tmpChunks); err != nil {
		return NewError(err, "Unable to delete temporary chunks")
	}

	tracelog.DebugLogger.Printf("Put %v done\n", name)

	return nil
}

func (folder *Folder) joinPath(one string, another string) string {
	if folder.normalizePrefix {
		return storage.JoinPath(one, another)
	}
	if one[len(one)-1] == '/' {
		one = one[:len(one)-1]
	}
	if another[0] == '/' {
		another = another[1:]
	}
	return one + "/" + another
}

// fillBuffer fills the buffer with data from the reader.
func fillBuffer(r io.Reader, b []byte) (int, error) {
	var (
		err       error
		n, offset int
	)

	for offset < len(b) {
		n, err = r.Read(b[offset:])
		offset += n
		if err != nil {
			break
		}
	}

	return offset, err
}

// getJitterDelay calculates an equal jitter delay.
func getJitterDelay(delay time.Duration) time.Duration {
	return time.Duration(rand.Float64()*float64(delay)) + delay
}

// minDuration returns the minimum value of provided durations.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}
