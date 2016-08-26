from builtins import str
from builtins import range
import logging
import logging.config
import threading
import copy
import traceback
import yaml
import json
import redis
import uuid
import sys, traceback

import pika

from biomaj_download.download.ftp import FTPDownload
from biomaj_download.download.http import HTTPDownload
from biomaj_download.download.direct import MultiDownload, DirectFTPDownload, DirectHttpDownload
from biomaj_download.download.localcopy import LocalDownload
from biomaj_download.message import message_pb2

class DownloadService(object):

    def __init__(self, config_file):
        with open(config_file, 'r') as ymlfile:
            self.config = yaml.load(ymlfile)

        logging.basicConfig(level=logging.DEBUG)

        self.logger = logging
        if 'log_config' in self.config:
            for handler in list(self.config['log_config']['handlers'].keys()):
                self.config['log_config']['handlers'][handler] = dict(self.config['log_config']['handlers'][handler])
            logging.config.dictConfig(self.config['log_config'])
            self.logger = logging.getLogger('biomaj')

        self.logger.debug('Init redis connection')
        self.redis_client = redis.StrictRedis(host=self.config['redis']['host'],
                                              port=self.config['redis']['port'],
                                              db=self.config['redis']['db'],
                                              decode_responses=True)

        connection = pika.BlockingConnection(pika.ConnectionParameters(
                            self.config['rabbitmq']['host']))
        self.channel = connection.channel()
        self.logger.info('Download service started')

    def get_handler(self, biomaj_file_info):
        """
        Get a protocol download handler
        """
        protocol = biomaj_file_info.remote_file.protocol
        server = biomaj_file_info.remote_file.server
        remote_dir = biomaj_file_info.remote_file.remote_dir

        downloader = None
        protocol_name = message_pb2.DownloadFile.Protocol.Name(protocol).lower()
        if protocol in [0, 1]:
            downloader = FTPDownload(protocol_name, server, remote_dir)
        if protocol in [2, 3]:
            downloader = HTTPDownload(protocol_name, server, remote_dir, biomaj_file_info.remote_file.http_parse)
        if protocol == 7:
            downloader = LocalDownload(remote_dir)
        if protocol == 4:
            downloader = DirectFTPDownload('ftp', server, remote_dir)
        if protocol == 5:
            downloader = DirectHttpDownload('http', server, remote_dir)
        if protocol == 6:
            downloader = DirectHttpDownload('https', server, remote_dir)
        if downloader is not None:
            downloader.bank = biomaj_file_info.bank
        else:
            return None

        proxy = None
        if biomaj_file_info.proxy is not None:
            proxy = biomaj_file_info.proxy.proxy
            proxy_auth = biomaj_file_info.proxy.proxy_auth
        if proxy is not None and proxy:
            downloader.set_proxy(proxy, proxy_auth)

        timeout_download = biomaj_file_info.timeout_download
        if timeout_download is not None and timeout_download:
            downloader.timeout = timeout_download

        if biomaj_file_info.remote_file.credentials:
            downloader.set_credentials(biomaj_file_info.remote_file.credentials)

        if biomaj_file_info.remote_file.save_as:
            downloader.save_as = biomaj_file_info.remote_file.save_as

        if biomaj_file_info.remote_file.param:
            downloader.save_as = biomaj_file_info.remote_file.param

        remote_files = []
        for remote_file in biomaj_file_info.remote_file.files:
            remote_files.append({'name': remote_file.name, 'save_as': remote_file.save_as})
            self.logger.debug('%s request to download %s from %s://%s' % (biomaj_file_info.bank, remote_file.name, protocol_name, server))
        downloader.set_files_to_download(remote_files)

        return downloader

    def clean(self, biomaj_file_info):
        '''
        Clean session and download info
        '''
        self.logger.debug('Clean %s session %s' % ( biomaj_file_info.bank,  biomaj_file_info.session))
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error')
        self.redis_client.delete(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':progress')

    def create_session(self, bank):
        '''
        Creates a unique session
        '''
        session = str(uuid.uuid4())
        self.redis_client.set(self.config['redis']['prefix'] + ':' + bank + ':session:' + session, 1)
        self.logger.debug('Create %s new session %s' % (bank, session))
        return session

    def download(self, biomaj_file_info):
        '''
        Download files
        '''
        self.logger.debug('New download request %s session %s' % ( biomaj_file_info.bank,  biomaj_file_info.session))
        session = self.redis_client.get(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session)
        if not session:
            self.logger.debug('Session %s for bank %s has expired, skipping download of %s' % (biomaj_file_info.session, biomaj_file_info.bank, biomaj_file_info.remote_file.files))
            return
        download_handler = self.get_handler(biomaj_file_info)
        if download_handler is None:
            self.logger.error('Could not get a handler for %s with session %s' % (biomaj_file_info.bank, biomaj_file_info.session))
        try:
            download_handler.download(biomaj_file_info.local_dir)
        except Exception as e:
            self.logger.error('Download exception for bank %s and file %s: %s' % (biomaj_file_info.bank, biomaj_file_info.remote_file.files, str(e)))
            traceback.print_exc()
            self.redis_client.incr(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':error')
        else:
            self.logger.debug('End of download for %s session %s' % (biomaj_file_info.bank, biomaj_file_info.session))

        self.redis_client.incr(self.config['redis']['prefix'] + ':' + biomaj_file_info.bank + ':session:' + biomaj_file_info.session + ':progress')


    def ask_download(self, biomaj_info_file):
        self.channel.basic_publish(exchange='',
                              routing_key='biomajdownload',
                              body=biomaj_info_file.SerializeToString(),
                              properties=pika.BasicProperties(
                                 delivery_mode = 2, # make message persistent
                                 ))

    def callback_messages(self, ch, method, properties, body):
        '''
        Manage download and send ACK message
        '''
        try:
            message = message_pb2.DownloadFile()
            message.ParseFromString(body)
            self.logger.debug('Received message: %s' % (message))
            self.download(message)
        except Exception as e:
            self.logger.error('Error with message: %s' % (str(e)))
            traceback.print_exc()
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def wait_for_messages(self):
        '''
        Loop queue waiting for messages
        '''
        self.channel.queue_declare(queue='biomajdownload', durable=True)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.callback_messages, queue='biomajdownload')
        self.channel.start_consuming()
