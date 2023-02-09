lint:
	pipenv run pylint centralized_metadata -E
	.circleci/pylint

test:
	pipenv run pytest

