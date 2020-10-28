default: test

test:
	go test ./...

doc: README.md

.PHONY: default test

README.md: README.md.tpl $(wildcard *.go)
	becca -package $(subst $(GOPATH)/src/,,$(PWD))
