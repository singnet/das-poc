#!/bin/bash

while true
do
    count=`ls -1 /opt/couchbase_setup/new_das/*.das 2>/dev/null | wc -l`
    if [ $count != 0 ]
    then 
        flist=`ls /opt/couchbase_setup/new_das/*.das`
        for path in $flist
        do
            fname=$(basename -- "$path")
            das="${fname%.*}"
            echo "Setting new Couchbase bucket to DAS '$das'"
            couchbase-cli \
                bucket-create \
                -c localhost:8091 \
                -u "${DAS_DATABASE_USERNAME}" \
                -p "${DAS_DATABASE_PASSWORD}" \
                --bucket ${das} \
                --bucket-type couchbase \
                --bucket-ramsize "${DAS_COUCHBASE_BUCKET_RAMSIZE:-4086}"
            rm -f $path
        done
    fi 
    sleep 1
done
