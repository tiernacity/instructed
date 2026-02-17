.PHONY: build run test clean

## Build the 'todo' binary executable (escript)
build:
	gleam export erlang-shipment
	escript build_escript.erl

## Run the project via gleam
run:
	gleam run

## Run tests
test:
	gleam test

## Remove build artifacts and the binary
clean:
	rm -rf build todo
