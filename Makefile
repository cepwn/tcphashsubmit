.PHONY: build build-server build-client run-server run-client test test-race docker-up docker-down migrate clean

build: build-server build-client

build-server:
	go build -o bin/server ./cmd/server

build-client:
	go build -o bin/client ./cmd/client

build-consumer:
	go build -o bin/consumer ./cmd/consumer

run-server: build-server
	./bin/server

run-client: build-client
	./bin/client

run-consumer: build-consumer
	./bin/consumer

test:
	go test ./...

test-race:
	go test -race ./...

test-verbose:
	go test -v ./...

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-reset: docker-down
	docker volume rm tcphashsubmit_postgres_data tcphashsubmit_rabbitmq_data 2>/dev/null || true
	docker compose up -d

migrate: build-consumer
	go run ./cmd/consumer migrate

clean:
	rm -rf bin/
