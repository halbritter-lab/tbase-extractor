.PHONY: help install install-dev lint format type-check test test-cov clean pre-commit

help:
	@echo "Available commands:"
	@echo "  install      Install the package in development mode"
	@echo "  install-dev  Install the package with development dependencies"
	@echo "  lint         Run all linting tools (ruff, black --check, mypy)"
	@echo "  format       Format code with black and ruff"
	@echo "  type-check   Run mypy type checking"
	@echo "  test         Run tests with pytest"
	@echo "  test-cov     Run tests with coverage report"
	@echo "  pre-commit   Install and run pre-commit hooks"
	@echo "  clean        Clean up build artifacts and cache files"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

format:
	@echo "Formatting code with black..."
	black .
	@echo "Fixing imports and other issues with ruff..."
	ruff check --fix .

lint:
	@echo "Running ruff..."
	ruff check .
	@echo "Checking code formatting with black..."
	black --check --diff .
	@echo "Running flake8..."
	flake8 .
	@echo "Running mypy type checking..."
	mypy .

type-check:
	mypy .

test:
	pytest

test-cov:
	pytest --cov=tbase_extractor --cov-report=html --cov-report=term-missing

pre-commit:
	@echo "Installing pre-commit hooks..."
	pre-commit install
	@echo "Running pre-commit on all files..."
	pre-commit run --all-files

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete