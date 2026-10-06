# Build/test tasks.

BIN := distributed-rate-limiter
GO  := go

.PHONY: all build run test vet bench race clean

all: build

## build: compile the server binary
build:
	$(GO) build -o $(BIN) .

## run: build and start the server
run: build
	./$(BIN)

## test: race tests + vet
test: race vet

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

## bench: algorithm benchmarks with allocation counts
bench:
	$(GO) test -run=^$$ -bench=. -benchmem ./internal/ratelimit

clean:
	rm -f $(BIN) $(BIN).exe
