.PHONY: lint
lint:
	mypy ./
	flake8 ./
	pydocstyle ./

.PHONY: test
test:
	pytest
