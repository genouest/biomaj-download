import time
import logging

from biomaj_download.downloadservice import DownloadService
from biomaj_download.message import message_pb2

dserv = DownloadService('config.yml')

session = dserv.create_session('alu')
print("Session: %s" % (session))


message = message_pb2.DownloadFile()
message.bank = 'alu'
message.session = session
message.local_dir = '/tmp'
remote_file = message_pb2.DownloadFile.RemoteFile()
remote_file.protocol = 2
remote_file.server = 'ftp2.fr.debian.org'
remote_file.remote_dir = '/debian/dists/'
biomaj_file = remote_file.files.add()
biomaj_file.name = 'README'
message.remote_file.MergeFrom(remote_file)

dserv.ask_download(message)
logging.warn('Sleeping....')
time.sleep(20)

dserv.clean(message)
'''
'http', 'ftp2.fr.debian.org', '/debian/dists/'
file README
'''
