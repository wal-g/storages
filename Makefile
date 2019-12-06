lint:
	golangci-lint run -v

unittest:
	go list ./... | grep -Ev 'vendor|submodules|tmp' | xargs go vet
	go test -v $(TEST_MODIFIER) ./azure/
	go test -v $(TEST_MODIFIER) ./fs/
	go test -v $(TEST_MODIFIER) ./gcs/
	go test -v $(TEST_MODIFIER) ./s3/
	go test -v $(TEST_MODIFIER) ./storage
	go test -v $(TEST_MODIFIER) ./swift/
