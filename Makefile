.PHONY: run clean

run:
	docker compose run --rm --build dev

clean:
	docker compose down --remove-orphans
