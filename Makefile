.PHONY: check fmt-check test race build artifacts docs hooks-install

check: fmt-check
	go vet ./...
	go test ./...
	go run ./tools/checkrepo

fmt-check:
	@test -z "$$(gofmt -l cmd internal tools examples)" || { gofmt -l cmd internal tools examples; exit 1; }

test:
	go test ./...

race:
	go test -race ./...

build:
	go build -o bin/rewind ./cmd/rewind

artifacts:
	go run ./tools/checkrepo

docs:
	go run ./tools/checkdocs
	mdbook build docs
	go run ./tools/checkdocs --site docs/book

hooks-install:
	@current=$$(git config --get core.hooksPath || true); \
	if [ -n "$$current" ] && [ "$$current" != ".githooks" ]; then \
	  echo "Refusing to replace existing core.hooksPath: $$current" >&2; exit 1; \
	fi; \
	git config --local core.hooksPath .githooks
