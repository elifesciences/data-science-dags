# Prerequisites

To prepare the environment on Mac before downloading the Python packages, you might need to install the following:

`brew install pkg-config icu4c`

Then, add this to the PATH in the file `~/.zshrc` using the command below:

`export PATH="/usr/local/opt/icu4c/bin:$PATH"`

## Running Pipelines via Docker

To run pipelines via docker, the non-dev image need to be built first via:

```bash
make airflow-build
```

Some environment variables can be set to be able to run the pipelines with real data:

```bash
DATA_SCIENCE_SOURCE_DATASET=my_dev
DATA_SCIENCE_OUTPUT_DATASET=my_dev
```

### PeerScout Build Reviewing Editor Profiles via Docker

```bash
make data-hub-pipelines-run-peerscout-build-reviewing-editor-profiles
```

This may take several minutes to run.

### PeerScout Recommend Senior Editors via Docker

```bash
make data-hub-pipelines-run-peerscout-recommend-senior-editors MAX_MANUSCRIPTS=10
```

## PeerScout API

To test the api

```bash
make peerscout-api-start
```

```bash
curl \
    --request POST \
    --data '@peerscout_api/example-data/peerscout-api-request1.json' \
    http://localhost:8090/api/peerscout | jq .
```

to test empty abstract response:

```bash
curl \
    --request POST \
    --data '@peerscout_api/example-data/peerscout-api-request2.json' \
    http://localhost:8090/api/peerscout  | jq .
```

to test if the author not provide any suggestion or exclution to display 'Not Provided'.

```bash
curl \
    --request POST \
    --data '@peerscout_api/example-data/peerscout-api-request3.json' \
    http://localhost:8090/api/peerscout  | jq .
```
