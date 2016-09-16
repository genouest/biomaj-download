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
        # If biomaj_proxy 
        r = requests.get(proxy + '/api/download/status/download/' + bank + '/' + session)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
        result = r.json()
        # TODO else
        # launch pool of threads that pulls files to download from queue and finish when queue is empty
        return (result['progress'], result['errors'])

    def download_remote_file(self, message):
        # If biomaj_proxy
        self.ask_download(message)
        # TODO else add to queues


    def clean(self, bank, session, proxy):
        r = requests.delete(proxy + '/api/download/session/' + bank + '/' + session)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
