lint:
	golangci-lint run -c .golangci.yml ./...
.PHONY: lint

lint-ci:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.39.0
	./bin/golangci-lint run -c .golangci.yml ./...
.PHONY: lint-ci

test:
	go test --cover ./...
.PHONY: test

test-ci:
	curl -L https://codeclimate.com/downloads/test-reporter/test-reporter-latest-linux-amd64 > ./cc-test-reporter
	chmod +x ./cc-test-reporter
	./cc-test-reporter before-build
	go test -race -coverprofile c.out ./...
	sed -i -e 's/github.com\/ivanlemeshev\/proglog\///g' c.out
	./cc-test-reporter after-build
.PHONY: test-ci
