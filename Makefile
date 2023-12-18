.PHONY: all build-prod build-cons

all: build-prod build-cons

build-prod:
	go build -o producer ./cmd/producer

build-cons:
	go build -o consumer ./cmd/consumer