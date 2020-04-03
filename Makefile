DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

USER_ID = $(shell id -u)
GROUP_ID = $(shell id -g)

DATA_SCIENCE_DAGS_PROJECTS_HOME = $(shell dirname $(shell pwd))
DATA_SCIENCE_DAGS_JUPYTER_PORT = $(shell bash -c 'source .env && echo $$DATA_SCIENCE_DAGS_JUPYTER_PORT')

JUPYTER_DOCKER_COMPOSE = USER_ID="$(USER_ID)" GROUP_ID="$(GROUP_ID)" \
	DATA_SCIENCE_DAGS_JUPYTER_PORT="$(DATA_SCIENCE_DAGS_JUPYTER_PORT)" \
	DATA_SCIENCE_DAGS_PROJECTS_HOME="$(DATA_SCIENCE_DAGS_PROJECTS_HOME)" \
	$(DOCKER_COMPOSE)
JUPYTER_RUN = $(JUPYTER_DOCKER_COMPOSE) run --rm jupyter

PROJECT_FOLDER = /home/jovyan/data-science-dags
DEV_RUN = $(JUPYTER_DOCKER_COMPOSE) run --workdir=$(PROJECT_FOLDER) --rm jupyter

ARGS =


.PHONY: build


venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi


venv-create:
	python3 -m venv $(VENV)


dev-install:
	$(PIP) install -r requirements.dev.txt
	$(PIP) install -r requirements.jupyter.txt
	$(PIP) install -r requirements.notebook.txt
	$(PIP) install -e . --no-deps


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 data_science_dags tests setup.py


dev-pylint:
	$(PYTHON) -m pylint data_science_dags tests setup.py


dev-lint: dev-flake8 dev-pylint


dev-pytest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS)


dev-watch:
	$(PYTHON) -m pytest_watch -- -p no:cacheprovider -p no:warnings $(ARGS)


dev-test: dev-lint dev-pytest


dev-jupyter-configure:
	$(VENV)/bin/jupyter nbextension enable --py widgetsnbextension


dev-jupyter-start: .require-jupyter-vars dev-jupyter-configure
	$(VENV)/bin/jupyter lab -y --port=$(DATA_SCIENCE_DAGS_JUPYTER_PORT)


jupyter-build:
	chmod a+w .
	@if [ "$(NO_BUILD)" != "y" ]; then \
		$(JUPYTER_DOCKER_COMPOSE) build jupyter; \
	fi


jupyter-shell:
	$(JUPYTER_RUN) bash $(ARGS)


jupyter-exec:
	$(JUPYTER_DOCKER_COMPOSE) exec jupyter bash $(ARGS)


jupyter-container-update-dependencies:
	$(JUPYTER_DOCKER_COMPOSE) exec jupyter \
        pip install -r "$(PROJECT_FOLDER)/requirements.notebook.txt"


jupyter-print-url:
	@echo "jupyter url: http://localhost:$(DATA_SCIENCE_DAGS_JUPYTER_PORT)"


jupyter-start:
	$(JUPYTER_DOCKER_COMPOSE) up -d jupyter
	@$(MAKE) jupyter-print-url


jupyter-logs:
	$(JUPYTER_DOCKER_COMPOSE) logs -f jupyter


jupyter-stop:
	$(JUPYTER_DOCKER_COMPOSE) down


pylint:
	$(DEV_RUN) pylint data_science_dags tests setup.py


flake8:
	$(DEV_RUN) flake8 data_science_dags tests setup.py


pytest:
	$(DEV_RUN) pytest -p no:cacheprovider $(ARGS)


pytest-not-slow:
	@$(MAKE) ARGS="$(ARGS) $(NOT_SLOW_PYTEST_ARGS)" pytest


.watch:
	$(DEV_RUN) pytest-watch -- -p no:cacheprovider -p no:warnings $(ARGS)


watch-slow:
	@$(MAKE) .watch


watch:
	@$(MAKE) ARGS="$(ARGS) $(NOT_SLOW_PYTEST_ARGS)" .watch


lint: flake8 pylint


test: lint pytest


ci-build-and-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		jupyter-build test


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
