.PHONY: build build-linux build-linux-arm64 run test check benchmark fmt clean

build:
	go build -o build/poglytics-scraper ./cmd/poglytics-scraper

# Cross-compile for Linux: go-sqlite3 is cgo, so this needs a Linux C toolchain
# (zig cc). _LARGEFILE64_SOURCE fixes sqlite3's off64_t on musl (see
# build/Dockerfile); -fno-sanitize=all disables zig's UBSan, which fails to link.
build-linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
		CC="zig cc -target x86_64-linux-musl" \
		CGO_CFLAGS="-D_LARGEFILE64_SOURCE -fno-sanitize=all" \
		go build -ldflags '-extldflags "-static"' \
		-o build/poglytics-scraper-linux-amd64 ./cmd/poglytics-scraper

build-linux-arm64:
	CGO_ENABLED=1 GOOS=linux GOARCH=arm64 \
		CC="zig cc -target aarch64-linux-musl" \
		CGO_CFLAGS="-D_LARGEFILE64_SOURCE -fno-sanitize=all" \
		go build -ldflags '-extldflags "-static"' \
		-o build/poglytics-scraper-linux-arm64 ./cmd/poglytics-scraper

run: build
	./build/poglytics-scraper

test:
	go test -race ./...

check: test
	go vet ./...

benchmark:
	python3 scripts/benchmark.py $(BENCH_ARGS)

fmt:
	go fmt ./...

clean:
	rm -f build/poglytics-scraper build/poglytics-scraper-linux-amd64 build/poglytics-scraper-linux-arm64
