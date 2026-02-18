.PHONY: run clean

dev:
	docker compose run --rm --build dev

clean:
	docker compose down --remove-orphans
