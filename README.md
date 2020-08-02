# About

[![PyPI version](https://badge.fury.io/py/biomaj-download.svg)](https://badge.fury.io/py/biomaj-download)

Microservice to manage the downloads of biomaj.

A protobuf interface is available in biomaj_download/message/message_pb2.py to exchange messages between BioMAJ and the download service.
Messages go through RabbitMQ (to be installed).

Python3 support only, python2 support is dropped

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
This mechanism is configurable through 2 downloader-specific options (see [Download options](#download-options)): **stop_condition** and **wait_policy**.

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

# Host keys

When using the `sftp` protocol, `biomaj-download` must check the host key.
Those keys are stored in a file (for instance `~/.ssh/known_hosts`).

Two options are available to configure this:

  - **ssh_hosts_file** which sets the file to use
  - **ssh_new_host** which sets what to do for a new host

When the host and the key are found in the file, the connection is accepted.
If the host is found but the key missmatches, the connection is rejected
(this usually indicates a problem or a change of configuration on the remote server).
When the host is not found, the decision depends on the value of **ssh_new_host**:

  - `reject` means that the connection is rejected
  - `accept` means that the connection is accepted
  - `add` means that the connection is accepted and the key is added to the file

See the description of the options in [Download options](#download-options).

# Download options

Since version 3.0.26, you can use the `set_options` method to pass a dictionary of downloader-specific options.
The following list shows some options and their effect (the option to set is the key and the parameter is the associated value):

  * **stop_condition**:
    * parameter: an instance of Tenacity `stop_base` or a string (see [Retrying](#retrying)).
    * downloader(s): all (except `LocalDownload`).
    * effect: sets the condition on which we should stop retrying to download a file.
    * default: `stop_after_attempt(3)` (i.e. stop after 3 attempts).
    * note: introduced in version 3.2.1.
  * **wait_policy**:
    * parameter: an instance of Tenacity `wait_base` or a string (see [Retrying](#retrying)).
    * downloader(s): all (except `LocalDownload`).
    * effect: sets the wait policy between download attempts.
    * default: `wait_fixed(3)` (i.e. wait 3 seconds between attempts).
    * note: introduced in version 3.2.1.
  * **skip_check_uncompress**:
    * parameter: bool.
    * downloader(s): all (except `LocalDownload`).
    * effect: if true, don't test the archives after download.
    * default: false (i.e. test the archives).
  * **ssl_verifyhost**:
    * parameter: bool.
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: if false, don't check that the name of the remote server is the same than in the SSL certificate.
    * default: true (i.e. check host name).
    * note: it's generally a bad idea to disable this verification. However some servers are badly configured. See [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSL_VERIFYHOST.html) for the corresponding cURL option.
  * **ssl_verifypeer**:
    * parameter: bool.
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: if false, don't check the authenticity of the peer's certificate.
    * default: true (i.e. check authenticity).
    * note: it's generally a bad idea to disable this verification. However some servers are badly configured. See [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSL_VERIFYPEER.html) for the corresponding cURL option.
  * **ssl_server_cert**:
    * parameter: path of the certificate file.
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: use the certificate(s) in this file to verify the peer with.
    * default: use OS certificates.
    * note: see [here](https://curl.haxx.se/libcurl/c/CURLOPT_CAINFO.html) for the corresponding cURL option.
    * parameter: int.
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`).
    * effect: sets the interval, in seconds, that the operating system will wait between sending keepalive probes.
    * default: cURL default (60s at the time of this writing).
    * note: see [here](https://curl.haxx.se/libcurl/c/CURLOPT_TCP_KEEPINTVL.html) for the corresponding cURL option.
  * **ftp_method**:
    * parameter: one of `default`, `multicwd`, `nocwd`, `singlecwd` (case insensitive).
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`) - only used for `FTP(S)`.
    * effect: sets the method used to reach a file on a FTP(S) server (`nocwd` and `singlecwd` are usually faster but not always supported).
    * default: `default` (which is `multicwd` at the time of this writing as in cURL).
    * note: see [here](https://curl.haxx.se/libcurl/c/CURLOPT_FTP_FILEMETHOD.html) for the corresponding cURL option; introduced in version 3.1.2.
  * **ssh_hosts_file**:
    * parameter: path of the known hosts file.
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`) - only used for `SFTP`.
    * effect: sets the file used to read/store host keys for `SFTP`.
    * default: `~/.ssh/known_hosts` (where `~` is the home directory of the current user).
    * note: see [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSH_KNOWNHOSTS.html) for the corresponding cURL option and the option below; introduced in version 3.2.1.
  * **ssh_new_host**:
    * parameter: one of `reject`, `accept`, `add`.
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`) - only used for `SFTP`.
    * effect: sets the policy to use for an unknown host.
    * default: `reject` (i.e. refuse new hosts - you must add them in the file for instance with `ssh` or `sftp`).
    * note: see [here](https://curl.haxx.se/libcurl/c/CURLOPT_SSH_KEYFUNCTION.html) for the corresponding cURL option and the option above; introduced in version 3.2.1.
  * *allow_redirections*:
    * parameter: bool.
    * downloader(s): `CurlDownload` (and derived classes: `DirectFTPDownload`, `DirectHTTPDownload`) - only used for `HTTPS(S)`.
    * effect: sets the policy for `HTTP` redirections.
    * default: `true` (i.e. follow redirections).
    * note: see [here](https://curl.haxx.se/libcurl/c/CURLOPT_FOLLOWLOCATION.html) for the corresponding cURL option; introduced in version 3.2.3.

Those options can be set in bank properties.
See file `global.properties.example` in [biomaj module](https://github.com/genouest/biomaj).
