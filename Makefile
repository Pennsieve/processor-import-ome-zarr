.PHONY: venv install test test-cov lint pre-commit run clean

VENV_DIR = venv
PYTHON = $(VENV_DIR)/bin/python
PIP = $(VENV_DIR)/bin/pip

venv:
	python3 -m venv $(VENV_DIR)

install: venv
	$(PIP) install -r processor/requirements.txt
	$(PIP) install -r requirements-test.txt

test:
	$(PYTHON) -m pytest tests/ -v

test-cov:
	$(PYTHON) -m pytest tests/ -v --cov=processor --cov-report=term-missing

lint:
	$(VENV_DIR)/bin/ruff check processor tests --fix
	$(VENV_DIR)/bin/ruff format processor tests

pre-commit:
	$(VENV_DIR)/bin/pre-commit install

run:
	docker compose up --build

clean:
	rm -rf data/input/*
	rm -rf data/output/*
