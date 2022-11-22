#!/bin/bash

DEBUG=0

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py create`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
DAS=$OUTPUT

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py load --url https://raw.githubusercontent.com/singnet/das/main/data/samples/animals.metta --das-key ${DAS}`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
sleep 5

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py check --das-key ${DAS}`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
if [ "${OUTPUT}" != "Ready" ]; then
    echo "check FAILED"
    exit 1
fi

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py count --das-key ${DAS}`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
if [ "${OUTPUT}" != "(14, 26)" ]; then
    echo "count FAILED"
    exit 1
fi

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py search_nodes --das-key ${DAS} --node-type Concept --node-name human`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
EXPECTED="['af12f10f9ae2002a1607ba0b47ba8407']"
if [ "$OUTPUT" != "$EXPECTED" ]; then
    echo "search_nodes FAILED"
    exit 1
fi

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py search_links --das-key ${DAS} --link-type Inheritance --targets "af12f10f9ae2002a1607ba0b47ba8407,bdfe4e7a431f73386f37c6448afe5840"`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
EXPECTED="['c93e1e758c53912638438e2a7d7f7b7f']"
if [ "$OUTPUT" != "$EXPECTED" ]; then
    echo "search_links FAILED"
    exit 1
fi

d1='$1'
d2='$2'
d3='$3'
OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py query --das-key ${DAS} --query "Node n1 Concept human, Link Inheritance n1 ${d2}"`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
EXPECTED="{{'${d2}': 'bdfe4e7a431f73386f37c6448afe5840'}}"
if [ "$OUTPUT" != "$EXPECTED" ]; then
    echo "query FAILED"
    exit 1
fi

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py atom --das-key ${DAS} --handle "bdfe4e7a431f73386f37c6448afe5840" --output-format DICT`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
if [ "${OUTPUT}" != "{'handle': 'bdfe4e7a431f73386f37c6448afe5840', 'type': 'Concept', 'name': 'mammal'}" ]; then
    echo "atom FAILED"
    exit 1
fi

docker exec -ti das_das_service_1 python3 service/client.py clear --das-key ${DAS}
sleep 5

OUTPUT=`docker exec -ti das_das_service_1 python3 service/client.py count --das-key ${DAS}`
OUTPUT=${OUTPUT//$'\r'/}
if [ $DEBUG == 1 ]; then echo ${OUTPUT}; fi
if [ "${OUTPUT}" != "(0, 0)" ]; then
    echo "clear FAILED"
    exit 1
fi
