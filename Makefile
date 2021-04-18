lint:
	golangci-lint run -c .golangci.yml ./...
.PHONY: lint

test:
	go test --cover ./...
.PHONY: test
