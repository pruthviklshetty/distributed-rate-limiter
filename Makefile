# Build/test tasks. `make build` compiles the frontend, then the single Go
# binary with the frontend embedded. On Windows without `make`, use build.ps1.

BIN := distributed-rate-limiter
GO  := go

.PHONY: all build web binary run test vet bench race clean docker-up docker-down

all: build

## build: frontend -> web/dist, then the embedding Go binary
build: web binary

## web: install deps and build the dashboard into web/dist
web:
	cd web && npm ci && npm run build

## binary: compile the Go binary (expects web/dist to exist)
binary:
	$(GO) build -o $(BIN) .

## run: build everything and start the server
run: build
	./$(BIN)

## test: race tests + vet (the per-stage "done" gate)
test: race vet

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

## bench: algorithm benchmarks with allocation counts
bench:
	$(GO) test -run=^$$ -bench=. -benchmem ./internal/ratelimit

## docker-up: app + Redis via Docker Compose
docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

clean:
	rm -rf $(BIN) $(BIN).exe web/dist web/node_modules
