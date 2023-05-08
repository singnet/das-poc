FROM python:3.9

ADD ./das /app/das
ADD ./tests /app/tests
ADD ./requirements.txt /app/requirements.txt
ADD ./data /data
ADD ./scripts /app/scripts
ADD ./service /app/service
ADD ./notebooks /app/notebooks

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir notebook

CMD [ "bash" ]
