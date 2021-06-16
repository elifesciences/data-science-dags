# Data Science DAGs

# PeerScout API

To test the access token

```bash
PEERSCOUT_API_ACCESS_TOKEN='123' make peerscout-api-start
```


To test the api

```bash
make peerscout-api-start
```

```bash
curl \
    --header 'X-Access-Token: 123' \
    --request POST \
    --data '@example-data/peerscout-api-request1.json' \
    http://localhost:8080/api/peerscout
```

or 

```bash
curl \
    --header 'X-Access-Token: 123' \
    --request POST \
    --data '@example-data/peerscout-api-request2.json' \
    http://localhost:8080/api/peerscout
```