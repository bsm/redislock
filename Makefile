default: test

test:
	go test ./...

test-race:
	go test ./... -race

lint:
	golangci-lint run

doc: README.md

README.md: README.md.tpl $(wildcard *.go)
	becca -package $(subst $(GOPATH)/src/,,$(PWD))
