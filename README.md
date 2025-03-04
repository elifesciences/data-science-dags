# Prerequisites

To prepare the environment on Mac before downloading the Python packages, you might need to install the following:

`brew install pkg-config icu4c`

Then, add this to the PATH in the file `~/.zshrc` using the command below:

`export PATH="/usr/local/opt/icu4c/bin:$PATH"`

## Running Pipelines via Virtual Environment

To run pipelines via a virtual environment make sure the virtual environment is created and dev dependencies are installed:

```bash
make dev-venv
```

To update:

```bash
make dev-install
```

Some environment variables can be set to be able to run the pipelines with real data:

```bash
DATA_SCIENCE_SOURCE_DATASET=my_dev
DATA_SCIENCE_OUTPUT_DATASET=my_dev
```

### PeerScout Build Senior Editor Profiles via Virtual Environment

```bash
make dev-run-peerscout-build-senior-editor-profiles
```

## Running Pipelines via Docker

To run pipelines via docker, the non-dev image need to be built first via:

```bash
make build
```

Some environment variables can be set to be able to run the pipelines with real data:

```bash
DATA_SCIENCE_SOURCE_DATASET=my_dev
DATA_SCIENCE_OUTPUT_DATASET=my_dev
```

### PeerScout Build Reviewing Editor Profiles via Docker

```bash
make data-science-pipelines-run-peerscout-build-reviewing-editor-profiles
```

This may take several minutes to run.

### PeerScout Build Senior Editor Profiles via Docker

```bash
make data-science-pipelines-run-peerscout-build-senior-editor-profiles
```

This may take several minutes to run.

### PeerScout Get Editor Pubmed Papers via Docker

```bash
make data-science-pipelines-run-peerscout-get-editor-pubmed-papers MAX_MANUSCRIPTS=10
```

This may take several minutes to run.

### PeerScout Recommend Reviewing Editors via Docker

```bash
make data-science-pipelines-run-peerscout-recommend-reviewing-editors MAX_MANUSCRIPTS=10
```

### PeerScout Recommend Senior Editors via Docker

```bash
make data-science-pipelines-run-peerscout-recommend-senior-editors MAX_MANUSCRIPTS=10
```

## PeerScout API

For sample requests also see:

- [docs/peerscout-api-requests-localhost.http](docs/peerscout-api-requests-localhost.http)
- [docs/peerscout-api-requests-deployed.http](docs/peerscout-api-requests-deployed.http)

To test the api

```bash
make peerscout-api-start
```

```bash
curl \
    --request POST \
    --data '@peerscout_api/example-data/peerscout-api-request1.json' \
    http://localhost:8080/api/peerscout | jq .
```

to test empty abstract response:

```bash
curl \
    --request POST \
    --data '@peerscout_api/example-data/peerscout-api-request2.json' \
    http://localhost:8080/api/peerscout  | jq .
```

to test if the author not provide any suggestion or exclution to display 'Not Provided'.

```bash
curl \
    --request POST \
    --data '@peerscout_api/example-data/peerscout-api-request3.json' \
    http://localhost:8080/api/peerscout  | jq .
```
