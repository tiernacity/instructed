.PHONY: dev clean check

dev:
	docker compose run --rm --build dev

## Build all Gleam projects with warnings as errors
check:
	cd instructed && gleam build --warnings-as-errors
	cd instructed-sqlite && gleam build --warnings-as-errors
	cd instructed-postgres && gleam build --warnings-as-errors
	cd example-todo/shared && gleam build --warnings-as-errors
	cd example-todo/server && gleam build --warnings-as-errors
	cd example-todo/client && gleam build --warnings-as-errors

clean:
	docker compose down --remove-orphans
