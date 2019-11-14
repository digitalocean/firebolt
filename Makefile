GIT_HASH = $(shell git describe --tags --dirty --always)

FIREBOLT_SRCS := $(shell find . -type f -iname '*.go' -not -path './pkg/*')

.PHONY: all docker push clean format lint test inttest cover

###
# phony targets
all: test

clean:
	@echo ">> Removing bin directory"
	@rm -rf bin
	@echo ">> Removing pkg/mod directory"
	@rm -rf pkg/mod
	@echo ">> Removing .makecache directory"
	@rm -rf .makecache

format:
	@go fmt
	@go get golang.org/x/tools/cmd/goimports
	@goimports -w $$(find . -name '*.go' | grep -v 'mock_*')

lint: .makecache .makecache/lint

test: lint .makecache/test

inttest: lint .makecache/inttest

cover: inttest .makecache/cover

###
# real targets

.makecache:
	@echo ">> Making directory $@"
	@mkdir $@

.makecache/lint: $(FIREBOLT_SRCS)
	@go get -u github.com/golangci/golangci-lint/cmd/golangci-lint@v1.18.0
	@golangci-lint run --no-config --disable-all -E gosec -E interfacer -E vet -E deadcode -E gocyclo -E golint -E varcheck -E dupl -E ineffassign -E unconvert -E nakedret -E gofmt -E unparam -E prealloc ./...

# Unit Tests only
.makecache/test: $(FIREBOLT_SRCS)
	@echo ">> Running tests"
	@go vet ./...
	@go test -race ./... --coverprofile=coverage.out
	@touch $@

# Unit and Integration Tests
.makecache/inttest: $(FIREBOLT_SRCS)
	@echo ">> Running integration tests"
	@docker-compose -f ./inttest/docker-compose.yml down -v
	@docker-compose -f ./inttest/docker-compose.yml up -d --force-recreate
	@go test ./... -tags=integration -coverpkg=./... -coverprofile=coverage.out
	@docker-compose -f ./inttest/docker-compose.yml down -v
	@touch $@

# Generate coverage report and an updated coverage badge
# Note that some non-production patterns like mocks are excluded by filtering them out of the coverage file with 'grep'
.makecache/cover: $(FIREBOLT_SRCS)
	@echo ">> Computing test coverage"
	@go get -u github.com/jpoles1/gopherbadger@1f4eedb7a3f6f2897d66f0aed24a5cc272a202a6
	@grep -Ev '/mock_|/internal|/inttest' coverage.out > coverage-nomocks.out
	@gopherbadger -covercmd "go tool cover -func=coverage-nomocks.out"
	@touch $@
