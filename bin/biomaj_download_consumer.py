'''
Message consumer for download requests
'''

import os
import logging

import requests
import yaml
import consul

from biomaj_download.downloadservice import DownloadService
from biomaj_core.utils import Utils

config_file = 'config.yml'
if 'BIOMAJ_CONFIG' in os.environ:
        config_file = os.environ['BIOMAJ_CONFIG']

config = None
with open(config_file, 'r') as ymlfile:
    config = yaml.load(ymlfile)
    Utils.service_config_override(config)


def on_download(bank, downloaded_files):
    metrics = []
    if 'prometheus' in config and not config['prometheus']:
        return
    if not downloaded_files:
        metric = {'bank': bank, 'error': 1}
        metrics.append(metrics)
    else:
        for downloaded_file in downloaded_files:
            metric = {'bank': bank}
            if 'error' in downloaded_file and downloaded_file['error']:
                metric['error'] = 1
            else:
                metric['size'] = downloaded_file['size']
                metric['download_time'] = downloaded_file['download_time']
            if 'hostname' in config['web']:
                metric['host'] = config['web']['hostname']
            metrics.append(metric)
        proxy = Utils.get_service_endpoint(config, 'download')
        r = requests.post(proxy + '/api/download/metrics', json = metrics)


download = DownloadService(config_file)
download.on_download_callback(on_download)
download.supervise()
download.wait_for_messages()
