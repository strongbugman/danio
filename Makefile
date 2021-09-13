all: test

version = `python -c 'import pkg_resources; print(pkg_resources.get_distribution("danio").version)'`

install:
	poetry install

test: install
	black . --check
	isort -c danio
	flake8 .
	mypy --ignore-missing-imports danio
	pytest --cov danio --cov-report term-missing

tag: install
	git tag $(version) -m "Release of version $(version)"

pypi_release: install
	poetry build
	poetry publish

github_release:
	git push && git push origin --tags

release: tag github_release pypi_release

clean:
	rm -rf .eggs *.egg-info dist build
