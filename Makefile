all: test

deps:
	@dep ensure && dep ensure -update
.PHONY: deps

test:
	@go test -v ./...
.PHONY: test
