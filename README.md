# Data Science DAGs

# PeerScout API

To test the access token

```bash
PEERSCOUT_API_ACCESS_TOKEN='123' make peerscout-api-start
```


```bash
curl \
    --header 'X-Access-Token: 123' \
    --request POST \
    --data '{"abstract":"output1"}' \
    http://localhost:8080/api/peerscout
```

or 

```bash
curl \
    --header 'X-Access-Token: 123' \
    --request POST \
    --data '{"abstract":"output2"}' \
    http://localhost:8080/api/peerscout
```