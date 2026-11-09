BINARY := vmrebalance
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_.-]+:.*?## ' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the binary to ./bin/vmrebalance
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o ./bin/$(BINARY) ./cmd/vmrebalance

.PHONY: test
test: ## Run unit tests with the race detector and coverage
	go test -race -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: e2e
e2e: ## Run the end-to-end test on a kind cluster (needs docker, kind, helm)
	./test/e2e/run.sh

.PHONY: vet
vet: ## Run go vet, also on the end-to-end test
	go vet ./...
	go vet -tags e2e ./test/e2e/

.PHONY: lint
lint: ## Run golangci-lint (skipped if not installed)
	@if command -v golangci-lint >/dev/null 2>&1; then \
	  golangci-lint run; \
	else \
	  echo "==> golangci-lint not installed locally; skipping"; \
	fi

.PHONY: check
check: vet test lint ## Pre-PR check: vet, test, lint

.PHONY: chart
chart: ## Lint the Helm chart with each ci/*-values.yaml and validate the manifests (kubeconform if installed)
	@for values in charts/vmrebalance/ci/*-values.yaml; do \
	  echo "==> $$values"; \
	  helm lint charts/vmrebalance --values "$$values" || exit 1; \
	  if command -v kubeconform >/dev/null 2>&1; then \
	    helm template vmrebalance charts/vmrebalance --namespace monitoring --values "$$values" \
	      | kubeconform -strict -summary -ignore-missing-schemas || exit 1; \
	  fi; \
	done

.PHONY: image
image: ## Build the container image vmrebalance:<version>
	docker build --build-arg VERSION=$(VERSION) -t $(BINARY):$(VERSION) .

.PHONY: tidy
tidy: ## Run go mod tidy
	go mod tidy

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf ./bin coverage.out
