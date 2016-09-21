from biomaj_download.downloadservice import DownloadService
import requests
import logging
import uuid
import time

import pika

from Queue import Queue
from biomaj_download.download.downloadthreads import DownloadThread

class DownloadClient(DownloadService):

    def __init__(self, rabbitmq_host, pool_size=5):
        self.logger = logging
        self.channel = None
        self.pool_size = pool_size
        self.proxy = None
        self.bank = None
        if rabbitmq_host:
            self.remote = True
            connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
            self.channel = connection.channel()
        else:
            self.remote = False
        self.logger.info("Use remote: %s" % (str(self.remote)))
        self.download_pool = []
        self.files_to_download = 0

    def set_queue_size(size):
        self.pool = Pool(size)

    def create_session(self, bank, proxy):
        if not self.remote:
            return str(uuid.uuid4())
        r = requests.post(proxy + '/api/download/session/' + bank)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
        result = r.json()
        self.session = result['session']
        self.proxy = proxy
        self.bank = bank
        return result['session']

    def download_status(self):
        # If biomaj_proxy
        r = requests.get(self.proxy + '/api/download/status/download/' + self.bank + '/' + self.session)
        if not r.status_code == 200:
            raise Exception('Failed to connect to the download proxy')
        result = r.json()
        # TODO else
        # launch pool of threads that pulls files to download from queue and finish when queue is empty
        return (result['progress'], result['errors'])

    def download_remote_file(self, operation):
        # If biomaj_proxy
        self.files_to_download += 1
        if self.remote:
            self.ask_download(operation)
        else:
            self.download_pool.append(operation.download)

    def _download_pool_files(self):
        thlist = []

        logging.info("Workflow:wf_download:Download:Threads:FillQueue")

        message_queue = Queue()
        for message in self.download_pool:
            message_queue.put(message)

        logging.info("Workflow:wf_download:Download:Threads:Start")

        for i in range(self.pool_size):
            th = DownloadThread(self, message_queue)
            thlist.append(th)
            th.start()

        message_queue.join()

        logging.info("Workflow:wf_download:Download:Threads:Over")
        nb_error = 0
        is_error = False
        nb_files_to_download = 0
        for th in thlist:
            nb_files_to_download += th.files_to_download
            if th.error > 0:
                is_error = True
                nb_error += 1
        return nb_error


    def wait_for_download(self):
        over = False
        nb_files_to_download = self.files_to_download
        logging.info("Workflow:wf_download:RemoteDownload:Waiting")
        if self.remote:
            download_error = False
            while not over:
                (progress, error) = self.download_status()
                if progress == nb_files_to_download:
                    over = True
                    logging.info("Workflow:wf_download:RemoteDownload:Completed:" + str(progress))
                    logging.info("Workflow:wf_download:RemoteDownload:Errors:" + str(error))
                else:
                    if progress % 10 == 0:
                        logging.info("Workflow:wf_download:RemoteDownload:InProgress:" + str(progress) + '/' + nb_files_to_download)
                    time.sleep(1)
                if error > 0:
                    download_error = True
            return download_error
        else:
            error = self._download_pool_files()
            logging.info('Workflow:wf_download:RemoteDownload:Completed')
            if error > 0:
                logging.info("Workflow:wf_download:RemoteDownload:Errors:" + str(error))
                return True
            else:
                return False

    def clean(self):
        if self.remote:
            r = requests.delete(self.proxy + '/api/download/session/' + self.bank + '/' + self.session)
            if not r.status_code == 200:
                raise Exception('Failed to connect to the download proxy')
