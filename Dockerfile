FROM python:3.9

ADD ./scripts /scripts
ADD ./data/bio_atomspace/bio_atomspace.tar.gz /data
ADD ./data/samples /data/samples

WORKDIR /scripts

RUN pip install -r requirements.txt

CMD [ "bash" ]
