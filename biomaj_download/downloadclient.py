from biomaj_download.downloadservice import DownloadService
import requests
import logging
import uuid
import time
import sys
import pika

from biomaj_download.download.downloadthreads import DownloadThread
from biomaj_download.message import downmessage_pb2

if sys.version_info[0] < 3:
    from Queue import Queue
else:
    from queue import Queue


class DownloadClient(DownloadService):

    def __init__(self, rabbitmq_host=None, rabbitmq_port=5672, rabbitmq_vhost='/', rabbitmq_user=None, rabbitmq_password=None, pool_size=5, redis_client=None, redis_prefix=None):
        self.logger = logging
        self.channel = None
        self.pool_size = pool_size
        self.proxy = None
        self.bank = None
        self.rate_limiting = 0
        self.redis_client = redis_client
        self.redis_prefix = redis_prefix
        if rabbitmq_host:
            self.remote = True
            connection = None
            if rabbitmq_user:
                credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
                connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, rabbitmq_vhost, credentials, heartbeat_interval=0))
            else:
                connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, rabbitmq_vhost, heartbeat_interval=0))
            self.channel = connection.channel()
        else:
            self.remote = False
        self.logger.info("Use remote: %s" % (str(self.remote)))
        self.download_pool = []
        self.files_to_download = 0

    def set_queue_size(self, size):
        self.pool_size = size

    def set_rate_limiting(self, rate):
        self.rate_limiting = rate

    def create_session(self, bank, proxy=None):
        self.bank = bank
        if not self.remote:
            self.session = str(uuid.uuid4())
            return self.session

        for i in range(3):
            try:
                url = proxy + '/api/download/session/' + bank
                r = requests.post(url)
                if r.status_code == 200:
                    result = r.json()
                    self.session = result['session']
                    self.proxy = proxy
                    return result['session']
            except Exception:
                logging.exception('Failed to send create operation: %s' % (url))
        raise Exception('Failed to connect to the download proxy')

    def download_status(self):
        '''
        Get progress of downloads, try to contact up to 3 times
        '''
        for i in range(2):
            try:
                url = self.proxy + '/api/download/status/download/' + self.bank + '/' + self.session
                r = requests.get(self.proxy + '/api/download/status/download/' + self.bank + '/' + self.session)
                if not r.status_code == 200:
                    logging.error('Failed to connect to the download proxy: %d' % (r.status_code))
                else:
                    result = r.json()
                    return (result['progress'], result['errors'])
            except Exception:
                logging.exception('Failed to connect to the download proxy: %s' % (url))
        raise Exception('Failed to connect to the download proxy')

    def download_remote_files(self, cf, downloaders, offline_dir):
        '''
        cf = Config
        downloaders = list of downloader
        offline_dir = base dir to download files

        '''
        for downloader in downloaders:
            for file_to_download in downloader.files_to_download:
                operation = downmessage_pb2.Operation()
                operation.type = 1
                message = downmessage_pb2.DownloadFile()
                message.bank = self.bank
                message.session = self.session
                message.local_dir = offline_dir
                remote_file = downmessage_pb2.DownloadFile.RemoteFile()
                protocol = downloader.protocol
                remote_file.protocol = downmessage_pb2.DownloadFile.Protocol.Value(protocol.upper())
                remote_file.server = downloader.server
                if cf.get('remote.dir'):
                    remote_file.remote_dir = cf.get('remote.dir')
                else:
                    remote_file.remote_dir = ''
                remote_file.credentials = downloader.credentials
                biomaj_file = remote_file.files.add()
                biomaj_file.name = file_to_download['name']
                if 'root' in file_to_download and file_to_download['root']:
                    biomaj_file.root = file_to_download['root']
                if 'param' in file_to_download and file_to_download['param']:
                    for key in list(file_to_download['param'].keys()):
                        param = remote_file.param.add()
                        param.name = key
                        param.value = file_to_download['param'][key]
                if 'save_as' in file_to_download and file_to_download['save_as']:
                    biomaj_file.save_as = file_to_download['save_as']
                if 'url' in file_to_download and file_to_download['url']:
                    biomaj_file.url = file_to_download['url']
                if 'permissions' in file_to_download and file_to_download['permissions']:
                    biomaj_file.metadata.permissions = file_to_download['permissions']
                if 'size' in file_to_download and file_to_download['size']:
                    biomaj_file.metadata.size = file_to_download['size']
                if 'year' in file_to_download and file_to_download['year']:
                    biomaj_file.metadata.year = file_to_download['year']
                if 'month' in file_to_download and file_to_download['month']:
                    biomaj_file.metadata.month = file_to_download['month']
                if 'day' in file_to_download and file_to_download['day']:
                    biomaj_file.metadata.day = file_to_download['day']
                if 'hash' in file_to_download and file_to_download['hash']:
                    biomaj_file.metadata.hash = file_to_download['hash']
                if 'md5' in file_to_download and file_to_download['md5']:
                    biomaj_file.metadata.md5 = file_to_download['md5']

                message.http_method = downmessage_pb2.DownloadFile.HTTP_METHOD.Value(downloader.method.upper())

                timeout_download = cf.get('timeout.download', None)
                if timeout_download:
                    try:
                        message.timeout_download = int(timeout_download)
                    except Exception:
                        logging.error('Invalid timeout value, not an integer, skipping')

                message.remote_file.MergeFrom(remote_file)
                operation.download.MergeFrom(message)
                self.download_remote_file(operation)

    def download_remote_file(self, operation):
        # If biomaj_proxy
        self.files_to_download += 1
        if self.remote:
            if self.rate_limiting > 0:
                self.download_pool.append(operation)
            else:
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
        nb_files_to_download = 0
        for th in thlist:
            nb_files_to_download += th.files_to_download
            if th.error > 0:
                nb_error += 1
        return nb_error

    def wait_for_download(self):
        over = False
        nb_files_to_download = self.files_to_download
        nb_submitted = 0
        logging.info("Workflow:wf_download:RemoteDownload:Waiting")
        if self.remote:
            download_error = False
            last_progress = 0
            while not over:
                # Check for cancel request
                if self.redis_client and self.redis_client.get(self.redis_prefix + ':' + self.bank + ':action:cancel'):
                    logging.warn('Cancel requested, stopping update')
                    self.redis_client.delete(self.redis_prefix + ':' + self.bank + ':action:cancel')
                    raise Exception('Cancel requested, stopping download')
                (progress, error) = self.download_status()
                logging.debug('Rate limiting: ' + str(self.rate_limiting))
                if self.rate_limiting > 0:
                    logging.debug('Workflow:wf_download:RemoteDownload:submitted: %d, current progress: %d, total: %d' % (nb_submitted, progress, nb_files_to_download))
                    if self.download_pool:
                        max_submit = self.rate_limiting
                        if nb_submitted != 0:
                            max_submit = self.rate_limiting - (nb_submitted - progress)
                        logging.debug('Workflow:wf_download:RemoteDownload:RequestAvailable:%d' % (max_submit))
                        for i in range(max_submit):
                            if self.download_pool:
                                logging.debug('Workflow:wf_download:RemoteDownload:RequestNewFile')
                                operation = self.download_pool.pop()
                                self.ask_download(operation)
                                nb_submitted += 1

                if progress >= nb_files_to_download:
                    over = True
                    logging.info("Workflow:wf_download:RemoteDownload:Completed:" + str(progress))
                    logging.info("Workflow:wf_download:RemoteDownload:Errors:" + str(error))
                else:
                    progress_percent = (progress // nb_files_to_download) * 100
                    if progress_percent > last_progress:
                        last_progress = progress_percent
                        logging.info("Workflow:wf_download:RemoteDownload:InProgress:" + str(progress) + '/' + str(nb_files_to_download) + "(" + str(progress_percent) + "%)")
                    time.sleep(10)
                if error > 0:
                    download_error = True
                    r = requests.get(self.proxy + '/api/download/error/download/' + self.bank + '/' + self.session)
                    if not r.status_code == 200:
                        raise Exception('Failed to connect to the download proxy')
                    result = r.json()
                    for err in result['error']:
                        logging.info("Workflow:wf_download:RemoteDownload:Errors:Info:" + str(err))
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
            for i in range(3):
                try:
                    url = self.proxy + '/api/download/session/' + self.bank + '/' + self.session
                    r = requests.delete(self.proxy + '/api/download/session/' + self.bank + '/' + self.session)
                    if r.status_code == 200:
                        return
                except Exception:
                    logging.exception('Failed to send clean operation: %s' % (url))
            raise Exception('Failed to connect to the download proxy')
