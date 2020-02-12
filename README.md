# About

[![PyPI version](https://badge.fury.io/py/biomaj-download.svg)](https://badge.fury.io/py/biomaj-download)

Microservice to manage the downloads of biomaj.

A protobuf interface is available in biomaj_download/message/message_pb2.py to exchange messages between BioMAJ and the download service.
Messages go through RabbitMQ (to be installed).

# Protobuf

To compile protobuf, in biomaj_download/message:

    protoc --python_out=. downmessage.proto

# Development

    flake8 --ignore E501 biomaj_download/\*.py biomaj_download/download

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

# Retrying

A common problem when downloading a large number of files is the handling of temporary failures (network issues, server too busy to answer, etc.).
Since version 3.1.2, `biomaj-download` uses the [Tenacity library](https://github.com/jd/tenacity) which is designed to handle this.

This mechanism is configurable through 2 downloader-specific options (see below): **stop_condition** and **wait_policy**.
When working on python code, you can pass instances of Tenacity's `stop_base` and `wait_base` respectively.
This includes classes defined in Tenacity or your own derived classes.

For bank configuration those options also parse strings read from the configuration file.
This parsing is based on the [Simple Eval library](https://github.com/danthedeckie/simpleeval).
The rules are straightforward:

  * All concrete stop and wait classes defined in Tenacity (i.e. classes inheriting from `stop_base` and `wait_base` respectively) can be used
    by calling their constructor with the expected parameters.
    For example, the string `"stop_after_attempt(5)"` will create the desired object.
	Note that stop and wait classes that need no argument must be used as constants (i.e. use `"stop_never"` and not `"stop_never()"`).
	Currently, this is the case for `"stop_never"` (as in Tenacity) and `"wait_none"` (this slightly differs from Tenacity where it is `"wait_none()"`).
  * You can use classes that allow to combine other stop conditions (namely `stop_all` and `stop_any`) or wait policies (namely `wait_combine`).
  * Operator `+` can be used to add wait policies (similar to `wait_combine`).
  * Operators `&` and `|` can be used to compose stop conditions (similar to `wait_all` and `wait_none` respectively).

However, in this case, you can't use your own conditions.
The complete list of stop conditions is:

* `stop_never` (although its use is discouraged)
* `stop_after_attempt`
* `stop_after_delay`
* `stop_when_event_set`
* `stop_all`
* `stop_any`

The complete list of wait policies is:

* `wait_none`
* `wait_fixed`
* `wait_random`
* `wait_incrementing`
* `wait_exponential`
* `wait_random_exponential`
* `wait_combine`
* `wait_chain`

Please refer to [Tenacity doc](https://tenacity.readthedocs.io/en/latest/) for their meaning and their parameters.

Examples (inspired by Tenacity doc):

  * `"wait_fixed(3) + wait_random(0, 2)"` and `"wait_combine(wait_fixed(3), wait_random(0, 2))"` are equivalent and will wait 3 seconds + up to 2 seconds of random delay
  * `"wait_chain(*([wait_fixed(3) for i in range(3)] + [wait_fixed(7) for i in range(2)] + [wait_fixed(9)]))"` will wait 3s for 3 attempts, 7s for the next 2 attempts and 9s for all attempts thereafter (here `+` is the list concatenation).
  * `"wait_none + wait_random(1,2)"` will wait between 1s and 2s (since `wait_none` doesn't wait).
  * `"stop_never | stop_after_attempt(5)"` will stop after 5 attempts (since `stop_never` never stops).

Note that some protocols (e.g. FTP) classify errors as temporary or permanent (for example trying to download inexisting file).
More generally, we could distinguish permanent errors based on error codes, etc. and not retry in this case.
However in our experience, so called permanent errors may well be temporary.
Therefore downloaders always retry whatever the error.
In some cases, this is a waste of time but generally this is worth it.

# Download options

Since version 3.0.26, you can use the `set_options` method to pass a dictionary of downloader-specific options.
The following list shows some options and their effect (the option to set is the key and the parameter is the associated value):

  * **stop_condition**:
    * parameter: an instance of Tenacity `stop_base` or a string (see above).
    * downloader(s): all (except LocalDownloader).
    * effect: sets the condition on which we should stop retrying to download a file.
    * default: .
  * **wait_policy**:
    * parameter: an instance of Tenacity `wait_base` or a string (see above).
    * downloader(s): all (except LocalDownloader).
    * effect: sets the wait policy between download trials.
    * default: .
  * **skip_check_uncompress**:
    * parameter: bool.
    * downloader(s): all (except LocalDownloader).
    * effect: If true, don't test the archives after download.
    * default: false (i.e. test the archives).
  * **ssl_verifyhost**:
    * parameter: bool.
    * downloader(s): `CurlDownloader` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: If false, don't check that the name of the remote server is the same than in the SSL certificate.
    * default: true (i.e. check host name).
    * note: It's generally a bad idea to disable this verification. However some servers are badly configured. See [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSL_VERIFYHOST.html) for the corresponding cURL option.
  * **ssl_verifypeer**:
    * parameter: bool.
    * downloader(s): `CurlDownloader` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: If false, don't check the authenticity of the peer's certificate.
    * default: true (i.e. check authenticity).
    * note: It's generally a bad idea to disable this verification. However some servers are badly configured. See [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSL_VERIFYPEER.html) for the corresponding cURL option.
  * **ssl_server_cert**:
    * parameter: filename of the certificate.
    * downloader(s): `CurlDownloader` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: Pass a file holding one or more certificates to verify the peer with.
    * default: use OS certificates.
    * note: See [here](https://curl.haxx.se/libcurl/c/CURLOPT_CAINFO.html) for the corresponding cURL option.
  * **tcp_keepalive**:
    * parameter: int.
    * downloader(s): `CurlDownloader` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: Sets the interval, in seconds, that the operating system will wait between sending keepalive probes.
    * default: cURL default (60s at the time of this writing).
    * note: See [here](https://curl.haxx.se/libcurl/c/CURLOPT_TCP_KEEPINTVL.html) for the corresponding cURL option.
  * **ftp_method**:
    * parameter: one of `default`, `multicwd`, `nocwd`, `singlecwd` (case insensitive).
    * downloader(s): `CurlDownloader` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: Sets the method to use to reach a file on a FTP(S) server (`nocwd` and `singlecwd` are usually faster but not always supported).
    * default: `default` (which is `multicwd` at the time of this writing)
    * note: See [here](https://curl.haxx.se/libcurl/c/CURLOPT_FTP_FILEMETHOD.html) for the corresponding cURL option.

Those options can be set in bank properties.
See file `global.properties.example` in [biomaj module](https://github.com/genouest/biomaj).
