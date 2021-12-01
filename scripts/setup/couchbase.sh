#!/bin/bash

is_ready() {
  local ATTEMPTS=$1
  local URL="localhost:8091/ui/index.html"
  local OUT=/tmp/is_ready.log

  for attempt in $(seq 1 $ATTEMPTS); do
    status=$(curl -s -w '%{http_code}' -o $OUT $URL)
    if [ "x$status" == "x200" ]; then
      echo "Couchbase is ready."
      return
    fi
    echo "Couchbase is starting up..."
    sleep 5
  done
  echo "Couchbase failed to be set up."
  return 1
}

# Wait for Couchbase
is_ready 5

docker-compose exec couchbase couchbase-cli \
  cluster-init \
  --cluster-name DAS_Cluster \
  --cluster-username "${DAS_DATABASE_USERNAME}" \
  --cluster-password "${DAS_DATABASE_PASSWORD}"

docker-compose exec couchbase couchbase-cli \
  bucket-create \
  -c localhost:8091 \
  -u "${DAS_DATABASE_USERNAME}" \
  -p "${DAS_DATABASE_PASSWORD}" \
  --bucket das \
  --bucket-type couchbase \
  --bucket-ramsize "${DAS_COUCHBASE_BUCKET_RAMSIZE}"
