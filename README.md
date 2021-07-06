# Data Science DAGs

# PeerScout API


To test the api

```bash
make peerscout-api-start
```

```bash
curl \
    --request POST \
    --data '@example-data/peerscout-api-request1.json' \
    http://localhost:8090/api/peerscout | jq .
```

to test empty abstract response:

```bash
curl \
    --request POST \
    --data '@example-data/peerscout-api-request2.json' \
    http://localhost:8090/api/peerscout  | jq .
```