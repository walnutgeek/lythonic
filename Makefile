# Makefile for easy development workflows.
# See development.md for docs.
# Note GitHub Actions call uv directly, not this Makefile.

.DEFAULT_GOAL := default

.PHONY: default install lint test upgrade build clean agent-rules docs docs-serve docs-deploy

default: agent-rules install lint test 

install:
	uv sync --all-extras

lint:
	uv run python devtools/lint.py

test:
	uv run pytest

upgrade:
	uv sync --upgrade --all-extras --dev

build:
	uv build


clean:
	-rm -rf dist/
	-rm -rf *.egg-info/
	-rm -rf .pytest_cache/
	-rm -rf .mypy_cache/
	-rm -rf .venv/
	-rm -rf site/
	-find . -type d -name "__pycache__" -exec rm -rf {} +

# Documentation
docs:
	uv run --group docs mkdocs build

docs-serve:
	uv run --group docs mkdocs serve

docs-deploy:
	uv run --group docs mkdocs gh-deploy
