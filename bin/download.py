'''
Web interface to query list/download status
Manage sessions and metrics
'''

import ssl
import os
import threading
import logging

import yaml
from flask import Flask
from flask import jsonify
from flask import g
from flask import request
from flask import Response
from prometheus_client import Counter
from prometheus_client import Gauge
from prometheus_client.exposition import generate_latest
import consul

from biomaj_download.message import message_pb2
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
    #download.channel.cancel()
    download.channel.stop_consuming('biomajdownload')
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

@app.route('/api/download/shutdown', methods=['POST'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'

@app.route('/api/download/metrics', methods=['GET'])
def metrics():
    return generate_latest()

@app.route('/api/download/metrics', methods=['POST'])
def add_metrics():
    '''
    Expects a JSON request with an array of {'bank': 'bank_name', 'error': 'error_message', 'size': size_of_download, 'download_time': seconds_to_download}
    '''
    downloaded_files = request.get_json()
    for downloaded_file in downloaded_files:
        if 'error' in downloaded_file and downloaded_file['error']:
            download_error_metric.labels(downloaded_file['bank']).inc()
        else:
            download_metric.labels(downloaded_file['bank']).inc()
            download_size_metric.labels(downloaded_file['bank']).set(downloaded_file['size'])
            download_time_metric.labels(downloaded_file['bank']).set(downloaded_file['download_time'])
    return jsonify({'msg': 'OK'})

@app.route('/api/download/status/list/<bank>/<session>')
def list_status(bank, session):
    '''
    Check if listing request is over
    '''
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = message_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    biomaj_file_info.local_dir = '/tmp'
    status = dserv.list_status(biomaj_file_info)
    return jsonify({'status': status})

@app.route('/api/download/status/download/<bank>/<session>')
def download_status(bank, session):
    '''
    Get number of downloads and errors for bank and session. Progress includes successful download and errored downloads.
    '''
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = message_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    biomaj_file_info.local_dir = '/tmp'
    (progress, errors) = dserv.download_status(biomaj_file_info)
    return jsonify({'progress': progress, 'errors': errors})

@app.route('/api/download/list/<bank>/<session>')
def list_result(bank, session):
    '''
    Get file listing for bank and session, using FileList protobuf serialized string
    '''
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = message_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    biomaj_file_info.local_dir = '/tmp'
    list_elts = dserv.list_result(biomaj_file_info, protobuf_decode=False)
    return jsonify({'files': list_elts})
    #return Response(list_elts, mimetype="application/x-protobuf")

@app.route('/api/download/session/<bank>', methods=['POST'])
def create_session(bank):
    dserv = DownloadService(config_file, rabbitmq=False)
    session = dserv._create_session(bank)
    return jsonify({'session': session})

@app.route('/api/download/session/<bank>/<session>', methods=['DELETE'])
def clean_session(bank, session):
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = message_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    dserv.clean(biomaj_file_info)
    return jsonify({'msg': 'session cleared'})


if __name__ == "__main__":
    start_server(config)
