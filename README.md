# About

Experimental (in progress) microservice to manage the downloads of biomaj.

A protobuf interface is available in biomaj_download/message/message_pb2.py to exchange messages between BioMAJ and the download service.
Messages go through RabbitMQ (to be installed).

# Protobuf

To compile protobuf, in biomaj_download/message:

protoc --python_out=. message.proto

# Development

    flake8  biomaj_download/*.py biomaj_download/download

# Prometheus metrics

Endpoint: /api/download/metrics


# Run

## Message consumer:
export BIOMAJ_CONFIG=path_to_config.yml
python bin/biomaj_download_consumer.py

## Web server

export BIOMAJ_CONFIG=path_to_config.yml
gunicorn biomaj_download.biomaj_download_web:app

Web processes should be behind a proxy/load balancer, API base url /api/download
