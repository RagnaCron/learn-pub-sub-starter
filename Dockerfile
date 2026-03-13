FROM rabbitmq:management-alpine

RUN rabbitmq-plugins enable rabbitmq_stomp

