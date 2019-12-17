# About

[![PyPI version](https://badge.fury.io/py/biomaj-download.svg)](https://badge.fury.io/py/biomaj-download)

Microservice to manage the downloads of biomaj.

A protobuf interface is available in biomaj_download/message/message_pb2.py to exchange messages between BioMAJ and the download service.
Messages go through RabbitMQ (to be installed).

# Protobuf

To compile protobuf, in biomaj_download/message:

    protoc --python_out=. downmessage.proto

# Development

    flake8  biomaj_download/\*.py biomaj_download/download

# Test

To run the test suite, use:

    nosetests -a '!local_irods' tests/biomaj_tests.py

This command skips the test that need a local iRODS server.

Some test might fail due to network connection. You can skip them with:

    nosetests -a '!network' tests/biomaj_tests.py

(To skip the local iRODS test and the network tests, use `-a '!network,!local_irods'`).

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
    rm -rf ..path_to/prometheus-multiproc
    mkdir -p ..path_to/prometheus-multiproc
    export prometheus_multiproc_dir=..path_to/prometheus-multiproc
    gunicorn -c gunicorn_conf.py biomaj_download.biomaj_download_web:app

Web processes should be behind a proxy/load balancer, API base url /api/download

Prometheus endpoint metrics are exposed via /metrics on web server

# Download options

Since version 3.0.26, you can use the `set_options` method to pass a dictionary of downloader-specific options.
The following list shows some options and their effect (the option to set is the key and the parameter is the associated value):

  * **skip_check_uncompress**:
    * parameter: bool.
    * downloader(s): all.
    * effect: If true, don't test the archives after download.
    * default: false (i.e. test the archives).
  * **ssl_verifyhost**:
    * parameter: bool.
    * downloader(s): `CurlDownloader`, `DirectFTPDownload`, `DirectHTTPDownload`.
    * effect: If false, don't check that the name of the remote server is the same than in the SSL certificate.
    * default: true (i.e. check host name).
    * note: It's generally a bad idea to disable this verification. However some servers are badly configured. See [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSL_VERIFYHOST.html) for the corresponding cURL option.
  * **ssl_verifypeer**:
    * parameter: bool.
    * downloader(s): `CurlDownloader`, `DirectFTPDownload`, `DirectHTTPDownload`.
    * effect: If false, don't check the authenticity of the peer's certificate.
    * default: true (i.e. check authenticity).
    * note: It's generally a bad idea to disable this verification. However some servers are badly configured. See [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSL_VERIFYPEER.html) for the corresponding cURL option.
  * **ssl_server_cert**:
    * parameter: filename of the certificate.
    * downloader(s): `CurlDownloader`, `DirectFTPDownload`, `DirectHTTPDownload`.
    * effect: Pass a file holding one or more certificates to verify the peer with.
    * default: use OS certificates.
    * note: See [here](https://curl.haxx.se/libcurl/c/CURLOPT_CAINFO.html) for the corresponding cURL option.
  * **tcp_keepalive**:
    * parameter: int.
    * downloader(s): `CurlDownloader`, `DirectFTPDownload`, `DirectHTTPDownload`.
    * effect: Sets the interval, in seconds, that the operating system will wait between sending keepalive probes.
    * default: cURL default (60s at the time of this writing).
    * note: See [here](https://curl.haxx.se/libcurl/c/CURLOPT_TCP_KEEPINTVL.html) for the corresponding cURL option.
  * **ftp_method**:
    * parameter: one of `default`, `multicwd`, `nocwd`, `singlecwd` (case insensitive).
    * downloader(s): `CurlDownloader`, `DirectFTPDownload`, `DirectHTTPDownload`.
    * effect: Sets the method to use to reach a file on a FTP(S) server (`nocwd` and `singlecwd` are usually faster but not always supported).
    * default: `default` (which is `multicwd` at the time of this writing)
    * note: See [here](https://curl.haxx.se/libcurl/c/CURLOPT_FTP_FILEMETHOD.html) for the corresponding cURL option.

Those options can be set in bank properties.
See file `global.properties.example` in [biomaj module](https://github.com/genouest/biomaj).
