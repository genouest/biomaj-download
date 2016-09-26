import os
import datetime
import logging
import logging.config
import yaml
import redis
import uuid
import traceback

import pika

from biomaj_download.download.ftp import FTPDownload
from biomaj_download.download.http import HTTPDownload
from biomaj_download.download.direct import DirectFTPDownload
from biomaj_download.download.direct import DirectHttpDownload
from biomaj_download.download.localcopy import LocalDownload
from biomaj_download.message import message_pb2


class DownloadService(object):

    channel = None
    redis_client = None

    def __init__(self, config_file=None, rabbitmq=True):
        self.logger = logging
        self.session = None
        self.bank = None
        self.download_callback = None
        with open(config_file, 'r') as ymlfile:
            self.config = yaml.load(ymlfile)

        if 'log_config' in self.config:
            for handler in list(self.config['log_config']['handlers'].keys()):
                self.config['log_config']['handlers'][handler] = dict(self.config['log_config']['handlers'][handler])
            logging.config.dictConfig(self.config['log_config'])
            self.logger = logging.getLogger('biomaj')

        if not self.redis_client:
            self.logger.debug('Init redis connection')
            self.redis_client = redis.StrictRedis(host=self.config['redis']['host'],
                                                  port=self.config['redis']['port'],
                                                  db=self.config['redis']['db'],
                                                  decode_responses=True)

        if rabbitmq and not self.channel:
            connection = pika.BlockingConnection(pika.ConnectionParameters(self.config['rabbitmq']['host']))
            self.channel = connection.channel()
            self.logger.info('Download service started')

    def close(self):
        if self.channel:
            self.channel.close()

    def on_download_callback(self, func):
        self.download_callback = func

    def get_handler(self, protocol_name, server, remote_dir, remote_files=[],
                    credentials=None, http_parse=None, http_method=None, param=None,
                    proxy=None, proxy_auth='',
                    save_as=None, timeout_download=None, offline_dir=None):
        protocol = message_pb2.DownloadFile.Protocol.Value(protocol_name.upper())
        downloader = None
        if protocol in [0, 1]:
            downloader = FTPDownload(protocol_name, server, remote_dir)
        if protocol in [2, 3]:
            downloader = HTTPDownload(protocol_name, server, remote_dir, http_parse)
        if protocol == 7:
            downloader = LocalDownload(remote_dir)
        if protocol == 4:
            downloader = DirectFTPDownload('ftp', server, '/')
        if protocol == 5:
            downloader = DirectHttpDownload('http', server, '/')
        if protocol == 6:
            downloader = DirectHttpDownload('https', server, '/')
        if downloader is None:
            return None

        for remote_file in remote_files:
            if remote_file['save_as']:
                save_as = remote_file['save_as']

        # For direct protocol, we only keep base name
        if protocol in [4, 5, 6]:
            tmp_remote = []
            for remote_file in remote_files:
                tmp_remote.append(remote_file['name'])
            remote_files = tmp_remote

        if http_method is not None:
            downloader.set_method(http_method)

        if offline_dir:
            downloader.set_offline_dir(offline_dir)

        if proxy is not None and proxy:
            downloader.set_proxy(proxy, proxy_auth)

        if timeout_download is not None and timeout_download:
            downloader.set_timeout(timeout_download)

        if credentials:
            downloader.set_credentials(credentials)

        if save_as:
            downloader.set_save_as(save_as)

        if param:
            downloader.set_param(param)

        downloader.set_server(server)

        downloader.set_protocol(protocol_name)

        downloader.logger = self.logger
        downloader.set_files_to_download(remote_files)
        return downloader

    def _get_handler(self, biomaj_file_info):
        """
        Get a protocol download handler
        """

        protocol = biomaj_file_info.remote_file.protocol
        server = biomaj_file_info.remote_file.server
        remote_dir = biomaj_file_info.remote_file.remote_dir

        protocol_name = message_pb2.DownloadFile.Protocol.Name(protocol).lower()
        self.logger.debug('%s request to download from %s://%s' % (biomaj_file_info.bank, protocol_name, server))

        remote_files = []
        for remote_file in biomaj_file_info.remote_file.files:
            remote_files.append({
                                'name': remote_file.name,
                                'save_as': remote_file.save_as,
                                'year': remote_file.metadata.year,
                                'month': remote_file.metadata.month,
                                'day': remote_file.metadata.day,
                                'root': remote_file.root
                                })

        proxy = None
        proxy_auth = ''
        if biomaj_file_info.proxy is not None:
            proxy = biomaj_file_info.proxy.proxy
            proxy_auth = biomaj_file_info.proxy.proxy_auth

        params = None
        if biomaj_file_info.remote_file.param:
            params = {}
            for param in biomaj_file_info.remote_file.param:
                params[param.name] = param.value
        return self.get_handler(protocol_name, server, remote_dir,
                                remote_files=remote_files,
                                credentials=biomaj_file_info.remote_file.credentials,
                                http_parse=biomaj_file_info.remote_file.http_parse,
                                http_method=message_pb2.DownloadFile.HTTP_METHOD.Name(biomaj_file_info.http_method),
                                param=params,
                                proxy=proxy,
                                proxy_auth=proxy_auth,
                                save_as=biomaj_file_info.remote_file.save_as,
                                timeout_download=biomaj_file_info.timeout_download,
                                offline_dir=biomaj_file_info.local_dir)

    def clean(self, biomaj_file_info=None):
        '''
        Clean session and download info
        '''
        session = self.session
        bank = self.bank
        if biomaj_file_info:
            session = biomaj_file_info.session
            bank = biomaj_file_info.bank

        self.logger.debug('Clean %s session %s' % (bank, session))
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session)
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session + ':error')
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session + ':progress')
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session + ':files')
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + bank + ':session:' + session + ':error:info')

    def _create_session(self, bank):
        '''
        Creates a unique session
        '''
        self.session = str(uuid.uuid4())
        self.redis_client.set(self.config['redis']['prefix'] + ':' + bank + ':session:' + self.session, 1)
        self.logger.debug('Create %s new session %s' % (bank, self.session))
        self.bank = bank
        return self.session

    def download_errors(self, biomaj_file_info):
        '''
        Get errors
        '''
        errors = []
        error = self.redis_client.rpop(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error:info')
        while error:
            errors.append(error)
            error = self.redis_client.rpop(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error:info')
        return errors

    def download_status(self, biomaj_file_info):
        '''
        Get current status
        '''

        error = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error')
        progress = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':progress')
        if error is None:
            error = -1
        if progress is None:
            progress = -1
        return (int(progress), int(error))

    def list_status(self, biomaj_file_info):

        list_progress = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':progress')
        if list_progress:
            return True
        else:
            return False

    def list_result(self, biomaj_file_info, protobuf_decode=True):
        '''
        Get file list result
        '''

        file_list = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':files')
        if protobuf_decode:
            file_list_pb2 = message_pb2.FileList()
            file_list_pb2.ParseFromString(file_list_pb2)
            return file_list_pb2

        return file_list

    def _list(self, download_handler, biomaj_file_info):
        '''
        List remote content, no session management
        '''
        file_list = []
        dir_list = []
        file_list_pb2 = message_pb2.FileList()

        try:
            (file_list, dir_list) = download_handler.list()
            download_handler.match(biomaj_file_info.remote_file.matches, file_list, dir_list)
        except Exception as e:
            self.logger.error('List exception for bank %s: %s' % (biomaj_file_info.bank, str(e)))
            self.redis_client.set(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error', 1)
            self.redis_client.lpush(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error:info', str(e))
        else:
            self.logger.debug('End of download for %s session %s' % (biomaj_file_info.bank, biomaj_file_info.session))
            for file_elt in download_handler.files_to_download:
                # file_pb2 = message_pb2.File()
                file_pb2 = file_list_pb2.files.add()
                file_pb2.name = file_elt['name']
                file_pb2.root = file_elt['root']
                if 'save_as' in file_elt:
                    file_pb2.save_as = file_elt['save_as']
                if 'url' in file_elt:
                    file_pb2.url = file_elt['url']
                if 'param' in file_elt and file_elt['param']:
                    for key in list(file_elt['param'].keys()):
                        param = file_list_pb2.param.add()
                        param.name = key
                        param.value = file_elt['param'][key]
                metadata = message_pb2.File.MetaData()
                metadata.permissions = file_elt['permissions']
                metadata.group = file_elt['group']
                metadata.size = int(file_elt['size'])
                metadata.hash = file_elt['hash']
                metadata.year = int(file_elt['year'])
                metadata.month = int(file_elt['month'])
                metadata.day = int(file_elt['day'])
                if 'format' in file_elt:
                    metadata.format = file_elt['format']
                file_pb2.metadata.MergeFrom(metadata)
        return file_list_pb2

    def list(self, biomaj_file_info):
        '''
        List remote content
        '''
        self.logger.debug('New list request %s session %s' % (biomaj_file_info.bank, biomaj_file_info.session))
        session = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session)
        if not session:
            self.logger.debug('Session %s for bank %s has expired, skipping download of %s' % (biomaj_file_info.session, biomaj_file_info.bank, biomaj_file_info.remote_file.files))
            return
        download_handler = self._get_handler(biomaj_file_info)
        if download_handler is None:
            self.logger.error('Could not get a handler for %s with session %s' % (biomaj_file_info.bank, biomaj_file_info.session))

        file_list_pb2 = self._list(download_handler, biomaj_file_info)

        self.redis_client.set(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':files', str(file_list_pb2.SerializeToString()))
        self.redis_client.incr(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':progress')

    def local_download(self, biomaj_file_info):
        '''
        Download files, no session
        '''
        download_handler = self._get_handler(biomaj_file_info)
        if download_handler is None:
            self.logger.error('Could not get a handler for %s with session %s' % (biomaj_file_info.bank, biomaj_file_info.session))
        downloaded_files = download_handler.download(biomaj_file_info.local_dir)
        self.logger.debug("Downloaded " + str(len(downloaded_files)) + " file in " + biomaj_file_info.local_dir)
        self.get_file_info(biomaj_file_info.local_dir, downloaded_files)
        return downloaded_files

    def download(self, biomaj_file_info):
        '''
        Download files

        Store in redis the progress and count of errors under:
         - prefix:bank_name:session:session_id:error
         - prefix:bank_name:session:session_id:progress
        '''

        self.logger.debug('New download request %s session %s' % (biomaj_file_info.bank, biomaj_file_info.session))
        session = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session)
        if not session:
            self.logger.debug('Session %s for bank %s has expired, skipping download of %s' % (biomaj_file_info.session, biomaj_file_info.bank, biomaj_file_info.remote_file.files))
            return
        downloaded_files = []
        try:
            downloaded_files = self.local_download(biomaj_file_info)
        except Exception as e:
            self.logger.error("Download error:%s:%s:%s" % (biomaj_file_info.bank, biomaj_file_info.session, str(e)))
            # traceback.print_exc()
            self.redis_client.incr(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error')
            self.redis_client.lpush(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error:info', str(e))
        else:
            self.logger.debug('End of download for %s session %s' % (biomaj_file_info.bank, biomaj_file_info.session))

        self.redis_client.incr(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':progress')
        return downloaded_files

    def get_file_info(self, local_dir, downloaded_files):
        if downloaded_files is None:
            return
        for downloaded_file in downloaded_files:
            file_dir = local_dir + '/' + os.path.dirname(downloaded_file['save_as'])
            fstat = os.stat(file_dir)
            downloaded_file['permissions'] = str(fstat.st_mode)
            downloaded_file['group'] = str(fstat.st_gid)
            downloaded_file['user'] = str(fstat.st_uid)
            downloaded_file['size'] = str(fstat.st_size)
            fstat_mtime = datetime.datetime.fromtimestamp(fstat.st_mtime)
            downloaded_file['month'] = fstat_mtime.month
            downloaded_file['day'] = fstat_mtime.day
            downloaded_file['year'] = fstat_mtime.year

    def ask_download(self, biomaj_info_file):
        self.channel.basic_publish(
            exchange='',
            routing_key='biomajdownload',
            body=biomaj_info_file.SerializeToString(),
            properties=pika.BasicProperties(
                # make message persistent
                delivery_mode=2
            ))

    def callback_messages(self, ch, method, properties, body):
        '''
        Manage download and send ACK message
        '''
        try:
            operation = message_pb2.Operation()
            operation.ParseFromString(body)
            message = operation.download
            self.logger.debug('Received message: %s' % (message))
            if operation.type == 0:
                message = operation.download
                self.logger.debug('List operation %s, %s' % (message.bank, message.session))
                if len(message.remote_file.matches) == 0:
                    self.logger.error('No pattern match for a list operation')
                else:
                    self.list(message)
            elif operation.type == 1:
                message = operation.download
                self.logger.debug('Download operation %s, %s' % (message.bank, message.session))
                downloaded_files = self.download(message)
                if self.download_callback is not None:
                    self.download_callback(message.bank, downloaded_files)
            else:
                self.logger.warn('Wrong message type, skipping')
        except Exception as e:
            self.logger.error('Error with message: %s' % (str(e)))
            traceback.print_exc()
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def wait_for_messages(self):
        '''
        Loop queue waiting for messages
        '''
        self.channel.queue_declare(queue='biomajdownload', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            self.callback_messages,
            queue='biomajdownload')
        self.channel.start_consuming()
