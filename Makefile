lint:
	pipenv run pylint centralized_metadata -E .circleci/pylint

test:
	PYTHONPATH=. pipenv run pytest

compare-dependencies:
	.circleci/scripts/compare_dependencies.sh

build-requirements:
	.circleci/scripts/build-requirements.sh

rebuild-pipfile: build-requirements
	pipenv --rm
	rm Pipfile.lock
	pipenv install --dev --requirements requirements.txt
