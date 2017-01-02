# About

Microservice to manage the downloads of biomaj.

A protobuf interface is available in biomaj_download/message/message_pb2.py to exchange messages between BioMAJ and the download service.
Messages go through RabbitMQ (to be installed).

# Protobuf

To compile protobuf, in biomaj_download/message:

    protoc --python_out=. message.proto

# Development

    flake8  biomaj_download/\*.py biomaj_download/download


# Run

## Message consumer:

    export BIOMAJ_CONFIG=path_to_config.yml
    python bin/biomaj_download_consumer.py

## Web server

If package is installed via pip, you need a file named *gunicorn_conf.py* containing somehwhere on local server:

    def worker_exit(server, worker):
        from prometheus_client import multiprocess
        multiprocess.mark_process_dead(worker.pid)

If you cloned the repository and installed it via python setup.py install, just refer to the *gunicorn_conf.py* in the cloned repository.


    export BIOMAJ_CONFIG=path_to_config.yml
    rm -rf ..path_to/godocker-prometheus-multiproc
    mkdir -p ..path_to/godocker-prometheus-multiproc
    export prometheus_multiproc_dir=..path_to/godocker-prometheus-multiproc
    gunicorn biomaj_download.biomaj_download_web:app

Web processes should be behind a proxy/load balancer, API base url /api/download
