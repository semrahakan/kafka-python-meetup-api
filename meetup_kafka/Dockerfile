FROM python:3.7

ENV APP meetup_kafka

#ADD . /app/meetup_kafka

COPY ./meetup_kafka/requirements.txt  .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /app/meetup_kafka

COPY ./meetup_kafka/test ./test
COPY ./meetup_kafka .

CMD sleep 5 ; python producer.py