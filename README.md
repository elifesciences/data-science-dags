# Data Science DAGs

# PeerScout API

To test the access token

PEERSCOUT_API_ACCESS_TOKEN='123' make peerscout-api-start


curl \
    --header "X-Access-Token: 123" \
    --request POST \
    --data '{"abstract":"abstract","body_html":"the body"}' \
    http://localhost:8080/api/peerscout