from biomaj_download.downloadservice import DownloadService
import requests
import logging
import pika

class DownloadClient(DownloadService):

    def __init__(self, rabbitmq_host):
        self.logger = logging
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = connection.channel()

    def create_session(self, bank, proxy):
        r = requests.post(proxy + '/api/download/session/' + bank)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
        result = r.json()
        return result['session']

    def download_status(self, bank, session, proxy):
        r = requests.get(proxy + '/api/download/status/download/' + bank + '/' + session)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
        result = r.json()
        return (result['progress'], result['errors'])

    def clean(self, bank, session, proxy):
        r = requests.delete(proxy + '/api/download/session/' + bank + '/' + session)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
