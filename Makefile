all: test

version = `python -c 'import pkg_resources; print(pkg_resources.get_distribution("danio").version)'`

.PHONY: install lint format test tag pypi_release github_release release clean

install:
	poetry install

lint:
	poetry run ruff check danio tests
	poetry run ruff format --check danio tests
	poetry run mypy --ignore-missing-imports danio

format:
	poetry run ruff check --fix danio tests
	poetry run ruff format danio tests

test: install
	poetry run ruff check danio tests
	poetry run ruff format --check danio tests
	poetry run mypy --ignore-missing-imports danio
	poetry run pytest --cov danio --cov-report term-missing tests/

tag: install
	git tag $(version) -m "Release of version $(version)"

pypi_release: install
	poetry build
	poetry publish

github_release:
	git push && git push origin --tags

release: tag github_release pypi_release

clean:
	rm -rf .eggs *.egg-info dist build .pytest_cache .coverage .ruff_cache
