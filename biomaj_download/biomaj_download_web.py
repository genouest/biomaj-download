'''
Web interface to query list/download status
Manage sessions and metrics
'''

import ssl
import os

import yaml
from flask import Flask
from flask import jsonify
from flask import request
from prometheus_client import Counter
from prometheus_client.exposition import generate_latest
from prometheus_client import multiprocess
from prometheus_client import CollectorRegistry
import consul

from biomaj_download.message import downmessage_pb2
from biomaj_download.downloadservice import DownloadService

from biomaj_core.utils import Utils

app = Flask(__name__)

download_metric = Counter("biomaj_download_total", "Bank total download.", ['bank'])
download_error_metric = Counter("biomaj_download_errors", "Bank total download errors.", ['bank'])
download_size_metric = Counter("biomaj_download_file_size", "Bank download file size in bytes.", ['bank', 'host'])
download_time_metric = Counter("biomaj_download_file_time", "Bank download file time in seconds.", ['bank', 'host'])

config_file = 'config.yml'
if 'BIOMAJ_CONFIG' in os.environ:
    config_file = os.environ['BIOMAJ_CONFIG']

config = None
with open(config_file, 'r') as ymlfile:
    config = yaml.load(ymlfile)
    Utils.service_config_override(config)


def consul_declare(config):
    if config['consul']['host']:
        consul_agent = consul.Consul(host=config['consul']['host'])
        consul_agent.agent.service.register(
            'biomaj-download',
            service_id=config['consul']['id'],
            address=config['web']['hostname'],
            port=config['web']['port'],
            tags=[
                'biomaj',
                'api',
                'traefik-int.backend=biomaj-download',
                'traefik-int.frontend.rule=PathPrefix:/api/download',
                'traefik-int.enable=true'
            ]
        )
        check = consul.Check.http(url='http://' + config['web']['hostname'] + ':' + str(config['web']['port']) + '/api/download', interval=20)
        consul_agent.agent.check.register(config['consul']['id'] + '_check', check=check, service_id=config['consul']['id'])


consul_declare(config)


@app.route('/api/download', methods=['GET'])
def ping():
    return jsonify({'msg': 'pong'})


@app.route('/metrics', methods=['GET'])
def metrics():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return generate_latest(registry)


@app.route('/api/download/metrics', methods=['POST'])
def add_metrics():
    '''
    Expects a JSON request with an array of {'bank': 'bank_name', 'host': 'hostname', 'error': 'error_message', 'size': size_of_download, 'download_time': seconds_to_download}
    '''
    downloaded_files = request.get_json()
    for downloaded_file in downloaded_files:
        host = 'na'
        if 'host' in downloaded_file:
            host = downloaded_file['host']
        if 'error' in downloaded_file and downloaded_file['error']:
            download_error_metric.labels(downloaded_file['bank']).inc()
        else:
            download_metric.labels(downloaded_file['bank']).inc()
            download_size_metric.labels(downloaded_file['bank'], host).inc(int(downloaded_file['size']))
            download_time_metric.labels(downloaded_file['bank'], host).inc(int(downloaded_file['download_time']))
    return jsonify({'msg': 'OK'})


@app.route('/api/download/status/list/<bank>/<session>')
def list_status(bank, session):
    '''
    Check if listing request is over
    '''
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = downmessage_pb2.DownloadFile()
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
    biomaj_file_info = downmessage_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    biomaj_file_info.local_dir = '/tmp'
    (progress, errors) = dserv.download_status(biomaj_file_info)
    return jsonify({'progress': progress, 'errors': errors})


@app.route('/api/download/error/download/<bank>/<session>')
def download_error(bank, session):
    '''
    Get errors info for bank and session
    '''
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = downmessage_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    biomaj_file_info.local_dir = '/tmp'
    errors = dserv.download_errors(biomaj_file_info)
    return jsonify({'error': errors})


@app.route('/api/download/list/<bank>/<session>')
def list_result(bank, session):
    '''
    Get file listing for bank and session, using FileList protobuf serialized string
    '''
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = downmessage_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    biomaj_file_info.local_dir = '/tmp'
    list_elts = dserv.list_result(biomaj_file_info, protobuf_decode=False)
    return jsonify({'files': list_elts})


@app.route('/api/download/session/<bank>', methods=['POST'])
def create_session(bank):
    dserv = DownloadService(config_file, rabbitmq=False)
    session = dserv._create_session(bank)
    return jsonify({'session': session})


@app.route('/api/download/session/<bank>/<session>', methods=['DELETE'])
def clean_session(bank, session):
    dserv = DownloadService(config_file, rabbitmq=False)
    biomaj_file_info = downmessage_pb2.DownloadFile()
    biomaj_file_info.bank = bank
    biomaj_file_info.session = session
    dserv.clean(biomaj_file_info)
    return jsonify({'msg': 'session cleared'})


if __name__ == "__main__":
    context = None
    if config['tls']['cert']:
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.load_cert_chain(config['tls']['cert'], config['tls']['key'])
    app.run(host='0.0.0.0', port=config['web']['port'], ssl_context=context, threaded=True, debug=config['web']['debug'])
