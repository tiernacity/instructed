.PHONY: build build-server build-client run-server run-client test clean

## Build both server and client
build: build-server build-client

## Build the server (Erlang shipment)
build-server:
	cd server && gleam build --warnings-as-errors && gleam export erlang-shipment

## Build the client (Deno/JS)
build-client:
	cd client && gleam build --warnings-as-errors

## Run the server (default port 8400)
run-server:
	./todo-server

## Run the client (pass ARGS, e.g. make run-client ARGS="add 'Buy milk' high")
run-client:
	./todo $(ARGS)

## Run tests
test:
	cd server && gleam test
	cd client && gleam test

## Remove build artifacts
clean:
	rm -rf server/build .todo
	rm -rf client/build
	rm -rf shared/build
