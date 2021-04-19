lint:
	golangci-lint run -c .golangci.yml ./...
.PHONY: lint

test:
	go test --cover -race ./...
.PHONY: test

proto-gen:
	(protoc api/v1/*.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.)
.PHONY: proto-gem
