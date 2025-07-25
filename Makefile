lint:
	pipenv run pylint centralized_metadata

test:
	PYTHONPATH=. pipenv run pytest

compare-dependencies:
	.github/scripts/compare_dependencies.sh

build-requirements:
	.github/scripts/build-requirements.sh

rebuild-pipfile: build-requirements
	pipenv --rm
	rm Pipfile.lock
	pipenv install --dev --requirements requirements.txt
