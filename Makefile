CODE_FOLDER := src/homework

.PHONY: install format lint run-and-clear run clear

install:
	poetry install

format:
	poetry run black --line-length 79 $(CODE_FOLDER)

lint:
	poetry run black  --line-length 79 --check $(CODE_FOLDER)
	poetry run flake8 $(CODE_FOLDER)
	poetry run mypy $(CODE_FOLDER)

run-and-clear: # Run the main script and clear the tables; it is used for multiple runs.
	poetry run python3 -m src.homework.main && poetry run python3 -m src.homework.clear_tables

run:
	poetry run python3 -m src.homework.main

clear:
	poetry run python3 -m src.homework.clear_tables