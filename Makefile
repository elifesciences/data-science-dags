DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml -f docker-compose.ci.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

USER_ID = $(shell id -u)
GROUP_ID = $(shell id -g)

DATA_SCIENCE_DAGS_PROJECTS_HOME = $(shell dirname $(shell pwd))
DATA_SCIENCE_DAGS_JUPYTER_PORT = $(shell bash -c 'source .env && echo $$DATA_SCIENCE_DAGS_JUPYTER_PORT')
DATA_SCIENCE_DAGS_PEERSCOUT_API_PORT = $(shell bash -c 'source .env && echo $$DATA_SCIENCE_DAGS_PEERSCOUT_API_PORT')

JUPYTER_DOCKER_COMPOSE = USER_ID="$(USER_ID)" GROUP_ID="$(GROUP_ID)" \
	DATA_SCIENCE_DAGS_JUPYTER_PORT="$(DATA_SCIENCE_DAGS_JUPYTER_PORT)" \
	DATA_SCIENCE_DAGS_PROJECTS_HOME="$(DATA_SCIENCE_DAGS_PROJECTS_HOME)" \
	$(DOCKER_COMPOSE)
JUPYTER_RUN = $(JUPYTER_DOCKER_COMPOSE) run --rm jupyter

PEERSCOUT_API_DOCKER_COMPOSE = DATA_SCIENCE_DAGS_PEERSCOUT_API_PORT="$(DATA_SCIENCE_DAGS_PEERSCOUT_API_PORT)" \
	$(DOCKER_COMPOSE)

PEERSCOUT_API_DEV_DOCKER_PYTHON = $(PEERSCOUT_API_DOCKER_COMPOSE) run --rm peerscout-api-dev python

PROJECT_FOLDER = /home/jovyan/data-science-dags
DEV_RUN = $(JUPYTER_DOCKER_COMPOSE) run --rm data-science-pipelines-dev

# Cells starts scrolling horizontally after 116 characters
NOTEBOOK_MAX_LINE_LENGTH = 116
NOTEBOOK_PYLINT_EXCLUSIONS = pointless-statement,expression-not-assigned,trailing-newlines,wrong-import-position,redefined-outer-name

OUTPUT_DATASET = data_science

MAX_MANUSCRIPTS = 10
MAX_EDITORS = 10

ARGS =


.PHONY: build

PYTEST_WATCH_MODULES = tests/unit_test

venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi


venv-create:
	python3 -m venv $(VENV)


dev-install:
	$(PIP) install --disable-pip-version-check -r requirements.build.txt
	$(PIP) install --disable-pip-version-check -r requirements.dev.txt
	$(PIP) install --disable-pip-version-check -r requirements.jupyter.txt
	$(PIP) install --disable-pip-version-check -r requirements.notebook.txt
	$(PIP) install --disable-pip-version-check -r requirements.prophet.txt
	$(PIP) install --disable-pip-version-check -e . --no-deps


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 data_science_pipeline peerscout_api tests tests_peerscout_api setup.py


dev-pylint:
	$(PYTHON) -m pylint data_science_pipeline peerscout_api tests tests_peerscout_api setup.py

dev-mypy:
	$(PYTHON) -m mypy data_science_pipeline peerscout_api tests tests_peerscout_api setup.py

dev-notebook-lint:
	$(VENV)/bin/jupyter nbconvert \
		--to=script \
		--output-dir=.temp/converted-notebooks/ \
		./notebooks/**/*.ipynb
	$(PYTHON) -m pylint .temp/converted-notebooks/*.py \
		--max-line-length=$(NOTEBOOK_MAX_LINE_LENGTH) \
		--disable=$(NOTEBOOK_PYLINT_EXCLUSIONS)
	$(PYTHON) -m mypy .temp/converted-notebooks/*.py


dev-notebook-nbstripout:
	$(VENV)/bin/nbstripout \
		$(shell find ./notebooks -name *.ipynb)


dev-nbstripout-status:
	$(VENV)/bin/nbstripout --status


dev-nbstripout-install:
	$(VENV)/bin/nbstripout --install


dev-lint: dev-flake8 dev-pylint dev-mypy dev-notebook-lint


dev-unittest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS) tests/unit_test


dev-watch:
	$(PYTHON) -m pytest_watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)


dev-test: dev-lint dev-unittest


dev-run-sample-notebook:
	$(PYTHON) -m papermill.cli \
		./notebooks/example.ipynb \
		/tmp/example-output.ipynb \
		-p output_dataset $(OUTPUT_DATASET)


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
	$(DEV_RUN) pylint data_science_pipeline peerscout_api tests setup.py


mypy:
	$(DEV_RUN) mypy data_science_pipeline peerscout_api tests setup.py

flake8:
	$(DEV_RUN) flake8 data_science_pipeline peerscout_api tests setup.py


notebook-lint:
	$(JUPYTER_RUN) bash -c '\
		cd .. \
		&& pwd \
		&& ls -l \
		&& ls -l ./notebooks \
		&& jupyter nbconvert \
		--to=script \
		--output-dir=./.temp/converted-notebooks/ \
		./notebooks/**/*.ipynb \
		&& pylint ./.temp/converted-notebooks/*.py \
		--max-line-length=$(NOTEBOOK_MAX_LINE_LENGTH) \
		--disable=$(NOTEBOOK_PYLINT_EXCLUSIONS) \
		&& mypy ./.temp/converted-notebooks/*.py \
	'


notebook-nbstripout-check:
	$(JUPYTER_RUN) bash -c '\
		cd .. \
		&& pwd \
		&& ls -l --recursive ./notebooks \
		&& rm -rf /tmp/notebooks-nbstripout \
		&& cp -r ./notebooks /tmp/notebooks-nbstripout \
		&& nbstripout $$(find ./notebooks -name *.ipynb) \
		&& ls -l --recursive /tmp/notebooks-nbstripout \
		&& diff --recursive --brief /tmp/notebooks-nbstripout ./notebooks \
		&& echo no unstripped notebook outputs found \
	'


unittest:
	$(DEV_RUN) pytest -p no:cacheprovider $(ARGS) tests/unit_test


unittest-not-slow:
	@$(MAKE) ARGS="$(ARGS) $(NOT_SLOW_PYTEST_ARGS)" unittest


.watch:
	$(DEV_RUN) pytest-watch -- -p no:cacheprovider $(ARGS) $(PYTEST_WATCH_MODULES)


watch-slow:
	@$(MAKE) .watch


watch:
	@$(MAKE) ARGS="$(ARGS) $(NOT_SLOW_PYTEST_ARGS)" .watch


lint: flake8 pylint mypy notebook-lint


test: lint unittest


build:
	$(DOCKER_COMPOSE) build data-science-pipelines

build-dev:
	$(DOCKER_COMPOSE) build data-science-pipelines-dev

shell-dev:
	$(DOCKER_COMPOSE) run --rm data-science-pipelines-dev bash


data-science-pipelines-run-peerscout-build-reviewing-editor-profiles:
	$(DOCKER_COMPOSE) run --rm data-science-pipelines \
		python -m data_science_pipeline.cli.peerscout_build_reviewing_editor_profiles


data-science-pipelines-run-peerscout-build-senior-editor-profiles:
	$(DOCKER_COMPOSE) run --rm data-science-pipelines \
		python -m data_science_pipeline.cli.peerscout_build_senior_editor_profiles


data-science-pipelines-run-peerscout-get-editor-pubmed-papers:
	$(DOCKER_COMPOSE) run --rm data-science-pipelines \
		python -m data_science_pipeline.cli.peerscout_get_editor_pubmed_papers \
			--max-editors=$(MAX_EDITORS) \
			--max-manuscripts=$(MAX_MANUSCRIPTS)


data-science-pipelines-run-peerscout-recommend-reviewing-editors:
	$(DOCKER_COMPOSE) run --rm data-science-pipelines \
		python -m data_science_pipeline.cli.peerscout_recommend_reviewing_editors \
			--max-manuscripts=$(MAX_MANUSCRIPTS)


data-science-pipelines-run-peerscout-recommend-senior-editors:
	$(DOCKER_COMPOSE) run --rm data-science-pipelines \
		python -m data_science_pipeline.cli.peerscout_recommend_senior_editors \
			--max-manuscripts=$(MAX_MANUSCRIPTS)


wait-for-peerscout-api:
	docker-compose run --rm wait-for-it \
		"peerscout-api:8080" \
		--timeout=30 \
		--strict\
		-- echo "PeerScout API is up"

peerscout-api-build:
	$(PEERSCOUT_API_DOCKER_COMPOSE) build peerscout-api

peerscout-api-start: 
	$(PEERSCOUT_API_DOCKER_COMPOSE) up -d peerscout-api

peerscout-api-start-and-wait:
	$(MAKE) peerscout-api-start
	$(MAKE) wait-for-peerscout-api

peerscout-api-end2end-test:
	$(PEERSCOUT_API_DEV_DOCKER_PYTHON) -m pytest ./tests_peerscout_api/end2end_tests -v

peerscout-api-start-and-end2end-test: \
	peerscout-api-start-and-wait peerscout-api-end2end-test

peerscout-api-stop:
	$(PEERSCOUT_API_DOCKER_COMPOSE) down

peerscout-api-dev-start: 
	$(PEERSCOUT_API_DOCKER_COMPOSE) run --rm -p '8090:8080' -e FLASK_ENV=development peerscout-api

peerscout-api-dev-build:
	$(PEERSCOUT_API_DOCKER_COMPOSE) build peerscout-api-dev

peerscout-api-dev-flake8:
	$(PEERSCOUT_API_DEV_DOCKER_PYTHON) -m flake8 tests_peerscout_api peerscout_api

peerscout-api-dev-pylint:
	$(PEERSCOUT_API_DEV_DOCKER_PYTHON) -m pylint tests_peerscout_api peerscout_api

peerscout-api-dev-mypy:
	$(PEERSCOUT_API_DEV_DOCKER_PYTHON) -m mypy tests_peerscout_api peerscout_api

peerscout-api-dev-lint: \
	peerscout-api-dev-flake8 peerscout-api-dev-pylint peerscout-api-dev-mypy

peerscout-api-dev-pytest:
	$(PEERSCOUT_API_DEV_DOCKER_PYTHON) -m pytest ./tests_peerscout_api/unit_tests -v

peerscout-api-dev-pytest-watch:
	$(PEERSCOUT_API_DEV_DOCKER_PYTHON) -m pytest_watch -- ./tests_peerscout_api/unit_tests -vv

peerscout-api-dev-test: \
	peerscout-api-dev-lint peerscout-api-dev-pytest


clean:
	$(DOCKER_COMPOSE) down -v


end2end-test:
	$(MAKE) clean
	$(DOCKER_COMPOSE) run --rm  test-client
	$(MAKE) clean


ci-build-main-image:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build

ci-test-exclude-e2e:
	$(DOCKER_COMPOSE) run --rm data-science-pipelines-dev ./run_test.sh


ci-build-and-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build \
		build-dev \
		jupyter-build \
		peerscout-api-build \
		notebook-lint \
		end2end-test \
		peerscout-api-dev-build \
		peerscout-api-dev-test \
		peerscout-api-start-and-end2end-test


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v


retag-push-image:
	docker tag  $(EXISTING_IMAGE_REPO):$(EXISTING_IMAGE_TAG) $(IMAGE_REPO):$(IMAGE_TAG)
	docker push  $(IMAGE_REPO):$(IMAGE_TAG)
