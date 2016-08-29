import ssl
import os
import threading
import logging

import yaml
from flask import Flask
from flask import g
from flask import request
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client.exposition import generate_latest
import consul


from biomaj_download.downloadservice import DownloadService

app = Flask(__name__)

download_metric = Counter("bank_download_total", "Bank total download.", ['bank'])
download_error_metric = Counter("bank_download_errors", "Bank total download errors.", ['bank'])
download_size_metric =  Gauge("bank_download_file_size", "Bank download file size in bytes.", ['bank'])
download_time_metric =  Gauge("bank_download_file_time", "Bank download file time in seconds.", ['bank'])

config_file = 'config.yml'
if 'BIOMAJ_CONFIG' in os.environ:
        config_file = os.environ['BIOMAJ_CONFIG']

config = None
with open(config_file, 'r') as ymlfile:
    config = yaml.load(ymlfile)


def on_download(bank, downloaded_files):
    logging.error(downloaded_files)
    for downloaded_file in downloaded_files:
        if 'error' in downloaded_file and downloaded_file['error']:
            download_error_metric.labels(bank).inc()
        else:
            download_metric.labels(bank).inc()
            download_size_metric.labels(bank).set(downloaded_file['size'])
            download_time_metric.labels(bank).set(downloaded_file['download_time'])


download = DownloadService(config_file)
download.on_download_callback(on_download)
mq_recieve_thread = threading.Thread(target=download.wait_for_messages)
mq_recieve_thread.start()


def start_server(config):
    context = None
    if config['tls']['cert']:
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.load_cert_chain(config['tls']['cert'], config['tls']['key'])

    if config['consul']['host']:
        consul_agent = consul.Consult(host=config['consul']['host'])
        consul_agent.agent.service.register('biomaj_download', service_id=config['consul']['id'], port=config['web']['port'], tags=['biomaj'])
        check = consul.Check.http(url=config['web']['local_endpoint'], interval=20)
        consul_agent.agent.check.register(name + '_check', check=check, service_id=config['consul']['id'])


    app.run(host='0.0.0.0', port=config['web']['port'], ssl_context=context, threaded=True, debug=config['web']['debug'])


def shutdown_server():
    download.channel.cancel()
    # download.channel.stop_consuming()
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

@app.route('/shutdown', methods=['POST'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'

@app.route('/metrics')
def metrics():
    return generate_latest()


if __name__ == "__main__":
    start_server(config)
