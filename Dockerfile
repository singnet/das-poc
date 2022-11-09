FROM python:3.9

ADD ./das /app/das
ADD ./tests /app/tests
ADD ./requirements.txt /app/requirements.txt
ADD ./data/bio_atomspace/bio_atomspace.tar.gz /data
ADD ./data/samples /data/samples
ADD ./scripts /app/scripts
ADD ./service /app/service

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

CMD [ "bash" ]
