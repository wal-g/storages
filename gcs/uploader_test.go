package gcs

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

// count how often we failed the first and the second chunk already
var failedFirst int
var failedSecond int

// total upload size
var totalSize int

func TestUploadChunk(t *testing.T) {

	tests := []struct {
		name       string
		failFirst  int // how often to fail the first 16MiB chunk
		failSecond int // how often to fail the second 16MiB chunk
		expectFail bool
	}{
		{"AllChunksSucceed", 0, 0, false},
		{"FirstChunkFailsOnce", 1, 0, false},
		{"SecondChunkFailsOnce", 0, 1, false},
		{"FirstAndSecondChunkFailOnce", 1, 1, false},
		{"FirstAndSecondChunkFail3times", 3, 3, false},
		//{"SecondChunkFails11times", 0, 11, false},
		{"FirstChunkFails11times", 11, 0, false},
	}

	for _, testcase := range tests {
		failedFirst = 0
		failedSecond = 0

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w := mockStorageWriter(ctx, t, testcase.failFirst, testcase.failSecond)

		u := NewUploader(w)
		u.maxUploadRetries = 3

		// nr of 20MiB chunks (we are simulating a 40MiB file)
		nrUploadChunks := 2
		totalSize = nrUploadChunks * defaultMaxChunkSize

		for i := 0; i < nrUploadChunks; i++ {
			dataChunk := u.allocateBuffer()
			n, err := rand.Read(dataChunk)
			if err != nil {
				t.Fatalf("Failed to fill chunk with random data: %v, %d", err, n)
			}

			chunk := chunk{
				name:  "chunk",
				index: 0,
				data:  dataChunk,
				size:  n,
			}

			if err := u.uploadChunk(ctx, chunk); err != nil {
				if !testcase.expectFail {
					t.Fatalf("Expected upload %s to succeed after %d first chunk errors and %d second chunk errors but uploader.uploadChunk() failed: %v",
						testcase.name, testcase.failFirst, testcase.failSecond, err)
				}
			} else {
				if testcase.expectFail {
					t.Fatalf("Expected upload %s to fail with %d first chunk errors and %d second chunk errors, but did not get an error.",
						testcase.name, testcase.failFirst, testcase.failSecond)
				}
			}

			u.resetBuffer(&dataChunk)
		}

		closeDone := make(chan error, 1)
		go func() {
			// Invoking w.Close() to ensure that this triggers completion of the upload.
			// writer.Write() is async, so we only can be sure that it succeeded after closing
			// the writer.
			closeDone <- w.Close()
		}()

		// Given that the ExponentialBackoff is 30 seconds from a start of 100ms,
		// let's wait for a maximum of 5 minutes to account for (2**n) increments
		// between [100ms, 30s].
		maxWait := 5 * time.Minute
		select {
		case <-time.After(maxWait):
			t.Fatalf("Test took longer than %s to return", maxWait)
		case err := <-closeDone:
			if testcase.expectFail {
				if err == nil {
					t.Fatalf("Expected upload %s to fail with %d first chunk errors and %d second chunk errors, but did not get an error.",
						testcase.name, testcase.failFirst, testcase.failSecond)
				}
			} else {
				if err != nil {
					t.Fatalf("Expected upload %s to succeed after %d first chunk errors and %d second chunk errors but finally failed when closing the Writer: %v",
						testcase.name, testcase.failFirst, testcase.failSecond, err)
				}
			}
		}
	}
}

type tokenSupplier int

func (ts *tokenSupplier) Token() (*oauth2.Token, error) {
	return &oauth2.Token{
		AccessToken:  "access-token",
		TokenType:    "Bearer",
		RefreshToken: "refresh-token",
		Expiry:       time.Now().Add(time.Hour),
	}, nil
}

// parseContentRange is determining where we are with the resumable upload calls
// by parsing the Content-Range header and is telling us if we should inject an error.
func parseContentRange(hdr http.Header, failFirstCount, failSecondCount int) (start string, completed, injectError bool) {
	cRange := strings.TrimPrefix(hdr.Get("Content-Range"), "bytes ")
	rangeSplits := strings.Split(cRange, "/")
	prelude := rangeSplits[0]

	// for debugging purposes - shows the current Content-Range we are trying to upload
	println("cRange: ", cRange)

	if strings.Contains(prelude, "0-") { // We did not finish the first 16MiB chunk yet
		if failFirstCount > 0 {
			if failedFirst < failFirstCount {
				// return a retryable error for this chunk.
				injectError = true
				failedFirst++
			}
		}
	} else { // We've already uploaded the first 16MiB chunk.
		if failSecondCount > 0 {
			if failedSecond < failSecondCount {
				// return a retryable error for this chunk.
				injectError = true
				failedSecond++
			}
		}
	}
	if len(prelude) == 0 || prelude == "*" || rangeSplits[1] == strconv.Itoa(totalSize) {
		// Completed the upload.
		completed = true
		return
	}
	startEndSplit := strings.Split(prelude, "-")
	start = startEndSplit[0]
	return
}

// mockStorageWriter is setting up a httptest Server which can inject 503
// responses and returns a storage.Writer pointing to its URL.
// We can setup how often it will inject errors for the first or second chunk.
func mockStorageWriter(ctx context.Context, t *testing.T, failFirstCount, failSecondCount int) *storage.Writer {
	uploadRoute := "/upload"

	var resumableUploadIDs atomic.Value
	resumableUploadIDs.Store(make(map[string]time.Time))

	lookupUploadID := func(resumableUploadID string) bool {
		_, ok := resumableUploadIDs.Load().(map[string]time.Time)[resumableUploadID]
		return ok
	}

	memoizeUploadID := func(resumableUploadID string) {
		resumableUploadIDs.Load().(map[string]time.Time)[resumableUploadID] = time.Now().UTC()
	}

	cst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resumableUploadID := r.URL.Query().Get("upload_id")
		path := r.URL.Path

		switch {
		case path == "/b": // Bucket creation
			w.Write([]byte(`{"kind":"storage#bucket","id":"bucket","name":"bucket"}`))
			return

		case (strings.HasPrefix(path, "/b/") || strings.HasPrefix(path, "/upload/storage/v1/b/")) && strings.HasSuffix(path, "/o"):
			if resumableUploadID == "" {
				uploadID := time.Now().Format(time.RFC3339Nano)
				w.Header().Set("X-GUploader-UploadID", uploadID)
				// construct the resumable upload URL for returning
				w.Header().Set("Location", fmt.Sprintf("http://%s?upload_id=%s", r.Host+uploadRoute, uploadID))
			} else {
				w.Write([]byte(`{"kind":"storage#object","bucket":"bucket","name":"bucket"}`))
			}
			return

		case path == uploadRoute:
			start, completedUpload, injectError := parseContentRange(r.Header, failFirstCount, failSecondCount)

			if resumableUploadID != "" {
				if !lookupUploadID(resumableUploadID) {
					if start == "0" {
						// First time that we are encountering this upload
						// and it is at byte 0, so memoize the uploadID.
						memoizeUploadID(resumableUploadID)
					} else {
						// this never should happen (starting a resumable upload with an offset != 0)
						errStr := fmt.Sprintf("mismatched_content_start (Invalid request. According to the Content-Range header,"+
							"the upload offset is %s byte(s), which exceeds already uploaded size of 0 byte(s).)\n%s", start, r.Header["Content-Range"])
						http.Error(w, errStr, http.StatusServiceUnavailable)
						return
					}
				}
			}
			if injectError {
				// inject 503 error
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if completedUpload {
				// Completed the upload. Return 200.
				ioutil.ReadAll(r.Body)
				w.Write([]byte(`{"kind":"storage#object","bucket":"bucket","name":"bucket"}`))
				return
			}

			// Return 308, because we are still expecting more parts.
			ioutil.ReadAll(r.Body)
			w.Header().Set("X-Http-Status-Code-Override", "308")
			return

		default:
			http.Error(w, "Unimplemented", http.StatusNotFound)
			return
		}
	}))

	hc := &http.Client{
		Transport: &oauth2.Transport{
			Source: new(tokenSupplier),
		},
	}

	opts := []option.ClientOption{option.WithHTTPClient(hc), option.WithEndpoint(cst.URL)}

	sc, err := storage.NewClient(ctx, opts...)
	if err != nil {
		t.Fatalf("Failed to create storage client: %v", err)
	}

	obj := sc.Bucket("mock-bucket").Object("object")
	return obj.NewWriter(ctx)
}
