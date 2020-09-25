package gcs

import (
	"bytes"
	"context"
	"io"
	"math"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

const (
	defaultMaxChunkSize = 20 << 20
)

type Uploader struct {
	writer           *storage.Writer
	objHandle        *storage.ObjectHandle
	maxChunkSize     int64
	writePosition    int64
	baseRetryDelay   time.Duration
	maxRetryDelay    time.Duration
	maxUploadRetries int
}

type UploaderOptions func(*Uploader)

type chunk struct {
	name  string
	index int
	data  []byte
	size  int
}

func NewUploader(writer *storage.Writer, objH *storage.ObjectHandle, options ...UploaderOptions) *Uploader {
	u := &Uploader{
		objHandle:        objH,
		writer:           writer,
		maxChunkSize:     defaultMaxChunkSize,
		baseRetryDelay:   BaseRetryDelay,
		maxRetryDelay:    maxRetryDelay,
		maxUploadRetries: MaxRetries,
	}

	for _, opt := range options {
		opt(u)
	}

	return u
}

func (u *Uploader) allocateBuffer() []byte {
	return make([]byte, u.maxChunkSize)
}

func (u *Uploader) resetBuffer(b *[]byte) {
	*b = u.allocateBuffer()
}

func (u *Uploader) uploadChunk(ctx context.Context, chunk chunk) error {
	timer := time.NewTimer(u.baseRetryDelay)
	defer func() {
		timer.Stop()
	}()

	u.writePosition = 0

	for retry := 0; retry <= u.maxUploadRetries; retry++ {
		if retry > 0 && retry%5 == 0 {
			if err := u.writer.Close(); err != nil {
				tracelog.ErrorLogger.Printf("Error on close writer: %v", err)
			}

			u.writer = u.objHandle.NewWriter(ctx)
			tracelog.InfoLogger.Println("Update writer")
		}

		bufReader := bytes.NewReader(chunk.data[u.writePosition:chunk.size])

		n, err := io.Copy(u.writer, bufReader)
		if err == nil {
			return nil
		}

		u.writePosition += n

		tracelog.ErrorLogger.Printf("Unable to copy to object %s, part %d, err: %v, retrying attempt %d", chunk.name, chunk.index, err, retry)

		tempDelay := u.baseRetryDelay * time.Duration(math.Exp2(float64(retry)))
		sleepInterval := minDuration(u.maxRetryDelay, getJitterDelay(tempDelay/2))

		timer.Reset(sleepInterval)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	return errors.Errorf("retry limit has been exceeded, total attempts: %d", u.maxUploadRetries)
}
