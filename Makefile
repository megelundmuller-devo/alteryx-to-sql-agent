.DEFAULT_GOAL := help

.PHONY: help install run test lint format

help:
	@echo "Available targets:"
	@echo "  install   Install dependencies using uv"
	@echo "  run       Run the application"
	@echo "  test      Run tests with pytest"
	@echo "  lint      Lint with ruff"
	@echo "  format    Format/fix with ruff"

install:
	uv venv
	uv pip install -r requirements.txt
	uv run pre-commit install

run:
	uv run python src/main.py

test:
	uv run pytest

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff check --fix src/ tests/
	uv run ruff format src/ tests/
