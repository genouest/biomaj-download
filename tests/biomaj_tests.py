from nose.tools import *
from nose.plugins.attrib import attr

import json
import shutil
import os
import tempfile
import logging
import copy
import stat
import time

from mock import patch

from optparse import OptionParser


from biomaj_core.config import BiomajConfig
from biomaj_core.utils import Utils
from biomaj_download.download.ftp import FTPDownload
from biomaj_download.download.direct import DirectFTPDownload, DirectHttpDownload
from biomaj_download.download.http import HTTPDownload, HTTPParse
from biomaj_download.download.localcopy  import LocalDownload
from biomaj_download.download.downloadthreads import DownloadThread
from biomaj_download.download.rsync import RSYNCDownload

import unittest

class UtilsForTest():
  """
  Copy properties files to a temp directory and update properties to
  use a temp directory
  """

  def __init__(self):
    """
    Setup the temp dirs and files.
    """
    self.global_properties = None
    self.bank_properties = None

    self.test_dir = tempfile.mkdtemp('biomaj')

    self.conf_dir =os.path.join(self.test_dir,'conf')
    if not os.path.exists(self.conf_dir):
      os.makedirs(self.conf_dir)
    self.data_dir =os.path.join(self.test_dir,'data')
    if not os.path.exists(self.data_dir):
      os.makedirs(self.data_dir)
    self.log_dir =os.path.join(self.test_dir,'log')
    if not os.path.exists(self.log_dir):
      os.makedirs(self.log_dir)
    self.process_dir =os.path.join(self.test_dir,'process')
    if not os.path.exists(self.process_dir):
      os.makedirs(self.process_dir)
    self.lock_dir =os.path.join(self.test_dir,'lock')
    if not os.path.exists(self.lock_dir):
      os.makedirs(self.lock_dir)
    self.cache_dir =os.path.join(self.test_dir,'cache')
    if not os.path.exists(self.cache_dir):
      os.makedirs(self.cache_dir)


    if self.global_properties is None:
      self.__copy_global_properties()

    if self.bank_properties is None:
      self.__copy_test_bank_properties()

  def clean(self):
    """
    Deletes temp directory
    """
    shutil.rmtree(self.test_dir)

  def __copy_test_bank_properties(self):
    if self.bank_properties is not None:
      return
    self.bank_properties = ['alu', 'local', 'testhttp','directhttp']
    curdir = os.path.dirname(os.path.realpath(__file__))
    for b in self.bank_properties:
        from_file = os.path.join(curdir, b+'.properties')
        to_file = os.path.join(self.conf_dir, b+'.properties')
        shutil.copyfile(from_file, to_file)

    self.bank_process = ['test.sh']
    curdir = os.path.dirname(os.path.realpath(__file__))
    procdir = os.path.join(curdir, 'bank/process')
    for proc in self.bank_process:
      from_file = os.path.join(procdir, proc)
      to_file = os.path.join(self.process_dir, proc)
      shutil.copyfile(from_file, to_file)
      os.chmod(to_file, stat.S_IRWXU)

    # Manage local bank test, use bank test subdir as remote
    properties = ['multi.properties', 'computederror.properties', 'error.properties', 'local.properties', 'localprocess.properties', 'testhttp.properties', 'computed.properties', 'computed2.properties', 'sub1.properties', 'sub2.properties']
    for prop in properties:
      from_file = os.path.join(curdir, prop)
      to_file = os.path.join(self.conf_dir, prop)
      fout = open(to_file,'w')
      with open(from_file,'r') as fin:
        for line in fin:
          if line.startswith('remote.dir'):
            fout.write("remote.dir="+os.path.join(curdir,'bank')+"\n")
          elif line.startswith('remote.files'):
            fout.write(line.replace('/tmp', os.path.join(curdir,'bank')))
          else:
            fout.write(line)
      fout.close()

  def __copy_global_properties(self):
    if self.global_properties is not None:
      return
    self.global_properties = os.path.join(self.conf_dir,'global.properties')
    curdir = os.path.dirname(os.path.realpath(__file__))
    global_template = os.path.join(curdir,'global.properties')
    fout = open(self.global_properties,'w')
    with open(global_template,'r') as fin:
        for line in fin:
          if line.startswith('conf.dir'):
            fout.write("conf.dir="+self.conf_dir+"\n")
          elif line.startswith('log.dir'):
            fout.write("log.dir="+self.log_dir+"\n")
          elif line.startswith('data.dir'):
            fout.write("data.dir="+self.data_dir+"\n")
          elif line.startswith('process.dir'):
            fout.write("process.dir="+self.process_dir+"\n")
          elif line.startswith('lock.dir'):
            fout.write("lock.dir="+self.lock_dir+"\n")
          else:
            fout.write(line)
    fout.close()


class TestBiomajUtils(unittest.TestCase):

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()


  def test_mimes(self):
    fasta_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),'bank/test2.fasta')
    (mime, encoding) = Utils.detect_format(fasta_file)
    self.assertTrue('application/fasta' == mime)

  @attr('compress')
  def test_uncompress(self):
    from_file = { 'root': os.path.dirname(os.path.realpath(__file__)),
                  'name': 'bank/test.fasta.gz'
                  }

    to_dir = self.utils.data_dir
    Utils.copy_files([from_file], to_dir)
    Utils.uncompress(os.path.join(to_dir, from_file['name']))
    self.assertTrue(os.path.exists(to_dir+'/bank/test.fasta'))

  def test_copy_with_regexp(self):
    from_dir = os.path.dirname(os.path.realpath(__file__))
    to_dir = self.utils.data_dir
    Utils.copy_files_with_regexp(from_dir, to_dir, ['.*\.py'])
    self.assertTrue(os.path.exists(to_dir+'/biomaj_tests.py'))

  def test_copy(self):
    from_dir = os.path.dirname(os.path.realpath(__file__))
    local_file = 'biomaj_tests.py'
    files_to_copy = [ {'root': from_dir, 'name': local_file}]
    to_dir = self.utils.data_dir
    Utils.copy_files(files_to_copy, to_dir)
    self.assertTrue(os.path.exists(to_dir+'/biomaj_tests.py'))

class TestBiomajLocalDownload(unittest.TestCase):
  """
  Test Local downloader
  """

  def setUp(self):
    self.utils = UtilsForTest()

    self.curdir = os.path.dirname(os.path.realpath(__file__))
    self.examples = os.path.join(self.curdir,'bank') + '/'

    BiomajConfig.load_config(self.utils.global_properties, allow_user_config=False)


  def tearDown(self):
    self.utils.clean()

  def test_local_list(self):
    locald = LocalDownload(self.examples)
    (file_list, dir_list) = locald.list()
    locald.close()
    self.assertTrue(len(file_list) > 1)

  def test_local_download(self):
    locald = LocalDownload(self.examples)
    (file_list, dir_list) = locald.list()
    locald.match([r'^test.*\.gz$'], file_list, dir_list)
    locald.download(self.utils.data_dir)
    locald.close()
    self.assertTrue(len(locald.files_to_download) == 1)

  def test_local_download_in_subdir(self):
    locald = LocalDownload(self.curdir+'/')
    (file_list, dir_list) = locald.list()
    locald.match([r'^/bank/test.*\.gz$'], file_list, dir_list)
    locald.download(self.utils.data_dir)
    locald.close()
    self.assertTrue(len(locald.files_to_download) == 1)


@attr('network')
@attr('http')
class TestBiomajHTTPDownload(unittest.TestCase):
  """
  Test HTTP downloader
  """
  def setUp(self):
    self.utils = UtilsForTest()
    BiomajConfig.load_config(self.utils.global_properties, allow_user_config=False)
    self.config = BiomajConfig('testhttp')
    self.http_parse = HTTPParse(self.config.get('http.parse.dir.line'),
        self.config.get('http.parse.file.line'),
        int(self.config.get('http.group.dir.name')),
        int(self.config.get('http.group.dir.date')),
        int(self.config.get('http.group.file.name')),
        int(self.config.get('http.group.file.date')),
        self.config.get('http.group.file.date_format', None),
        int(self.config.get('http.group.file.size'))
    )

  def tearDown(self):
    self.utils.clean()

  def test_http_list(self):
    httpd = HTTPDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_http_list_dateregexp(self):
    self.http_parse.file_date_format = "%%d-%%b-%%Y %%H:%%M"
    httpd = HTTPDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_http_download(self):
    httpd = HTTPDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.match([r'^README$'], file_list, dir_list)
    httpd.download(self.utils.data_dir)
    httpd.close()
    self.assertTrue(len(httpd.files_to_download) == 1)

  def test_http_download_in_subdir(self):
    httpd = HTTPDownload('http', 'ftp2.fr.debian.org', '/debian/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.match([r'^dists/README$'], file_list, dir_list)
    httpd.download(self.utils.data_dir)
    httpd.close()
    self.assertTrue(len(httpd.files_to_download) == 1)


@attr('directftp')
@attr('network')
class TestBiomajDirectFTPDownload(unittest.TestCase):
  """
  Test DirectFTP downloader
  """

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()

  def test_ftp_list(self):
    file_list = ['/blast/db/FASTA/alu.n.gz.md5']
    ftpd = DirectFTPDownload('ftp', 'ftp.ncbi.nih.gov', '')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_download(self):
    file_list = ['/blast/db/FASTA/alu.n.gz.md5']
    ftpd = DirectFTPDownload('ftp', 'ftp.ncbi.nih.gov', '')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'alu.n.gz.md5')))


@attr('directhttp')
@attr('network')
class TestBiomajDirectHTTPDownload(unittest.TestCase):
  """
  Test DirectFTP downloader
  """

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()

  def test_http_list(self):
    file_list = ['/debian/README.html']
    ftpd = DirectHttpDownload('http', 'ftp2.fr.debian.org', '')
    ftpd.set_files_to_download(file_list)
    fday = ftpd.files_to_download[0]['day']
    fmonth = ftpd.files_to_download[0]['month']
    fyear = ftpd.files_to_download[0]['year']
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) == 1)
    self.assertTrue(file_list[0]['size']!=0)
    self.assertFalse(fyear == ftpd.files_to_download[0]['year'] and fmonth == ftpd.files_to_download[0]['month'] and fday == ftpd.files_to_download[0]['day'])

  def test_download(self):
    file_list = ['/debian/README.html']
    ftpd = DirectHttpDownload('http', 'ftp2.fr.debian.org', '')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'README.html')))

  def test_download_get_params_save_as(self):
    file_list = ['/get']
    ftpd = DirectHttpDownload('http', 'httpbin.org', '')
    ftpd.set_files_to_download(file_list)
    ftpd.param = { 'key1': 'value1', 'key2': 'value2'}
    ftpd.save_as = 'test.json'
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'test.json')))
    with open(os.path.join(self.utils.data_dir,'test.json'), 'r') as content_file:
      content = content_file.read()
      my_json = json.loads(content)
      self.assertTrue(my_json['args']['key1'] == 'value1')

  @attr('test')
  def test_download_save_as(self):
    file_list = ['/debian/README.html']
    ftpd = DirectHttpDownload('http', 'ftp2.fr.debian.org', '')
    ftpd.set_files_to_download(file_list)
    ftpd.save_as = 'test.html'
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'test.html')))

  def test_download_post_params(self):
    #file_list = ['/debian/README.html']
    file_list = ['/post']
    ftpd = DirectHttpDownload('http', 'httpbin.org', '')
    ftpd.set_files_to_download(file_list)
    ftpd.param = { 'key1': 'value1', 'key2': 'value2'}
    ftpd.save_as = 'test.json'
    ftpd.method = 'POST'
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'test.json')))
    with open(os.path.join(self.utils.data_dir,'test.json'), 'r') as content_file:
      content = content_file.read()
      my_json = json.loads(content)
      self.assertTrue(my_json['form']['key1'] == 'value1')


@attr('ftp')
@attr('network')
class TestBiomajFTPDownload(unittest.TestCase):
  """
  Test FTP downloader
  """

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()

  def test_ftp_list(self):
    ftpd = FTPDownload('ftp', 'speedtest.tele2.net', '/')
    # ftpd = FTPDownload('ftp', 'ftp.ncbi.nih.gov', '/blast/db/FASTA/')
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) > 1)

  @attr('test')
  def test_download(self):
    # ftpd = FTPDownload('ftp', 'ftp.ncbi.nih.gov', '/blast/db/FASTA/')
    ftpd = FTPDownload('ftp', 'speedtest.tele2.net', '/')
    (file_list, dir_list) = ftpd.list()
    # ftpd.match([r'^alu.*\.gz$'], file_list, dir_list)
    ftpd.match([r'^1.*KB\.zip$'], file_list, dir_list)
    ftpd.download(self.utils.data_dir)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download) == 2)

  def test_download_in_subdir(self):
    ftpd = FTPDownload('ftp', 'ftp.ncbi.nih.gov', '/blast/')
    (file_list, dir_list) = ftpd.list()
    try:
        ftpd.match([r'^db/FASTA/alu.*\.gz$'], file_list, dir_list)
    except Exception as e:
        print("Error: " + str(e))
        self.skipTest("Skipping test due to remote server error")
    ftpd.download(self.utils.data_dir)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download) == 2)

  def test_download_or_copy(self):
    ftpd = FTPDownload('ftp', 'ftp.ncbi.nih.gov', '/blast/')
    ftpd.files_to_download = [
          {'name':'/test1', 'year': '2013', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test2', 'year': '2013', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test/test1', 'year': '2013', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test/test11', 'year': '2013', 'month': '11', 'day': '10', 'size': 10}
          ]
    available_files = [
          {'name':'/test1', 'year': '2013', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test12', 'year': '2013', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test3', 'year': '2013', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test/test1', 'year': '2013', 'month': '11', 'day': '10', 'size': 20},
          {'name':'/test/test11', 'year': '2013', 'month': '11', 'day': '10', 'size': 10}
          ]
    ftpd.download_or_copy(available_files, '/biomaj', False)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download)==2)
    self.assertTrue(len(ftpd.files_to_copy)==2)

  def test_get_more_recent_file(self):
    files = [
          {'name':'/test1', 'year': '2013', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test2', 'year': '2013', 'month': '11', 'day': '12', 'size': 10},
          {'name':'/test/test1', 'year': '1988', 'month': '11', 'day': '10', 'size': 10},
          {'name':'/test/test11', 'year': '2013', 'month': '9', 'day': '23', 'size': 10}
          ]
    release = Utils.get_more_recent_file(files)
    self.assertTrue(release['year']=='2013')
    self.assertTrue(release['month']=='11')
    self.assertTrue(release['day']=='12')
    
@attr('rsync')
@attr('local')
class TestBiomajRSYNCDownload(unittest.TestCase):
    '''
    Test RSYNC downloader
    '''
    def setUp(self):
        self.utils = UtilsForTest()

        self.curdir = os.path.dirname(os.path.realpath(__file__))
        self.examples = os.path.join(self.curdir,'bank') + '/'
        BiomajConfig.load_config(self.utils.global_properties, allow_user_config=False)

        
    def tearDown(self):
        self.utils.clean()
    
    def test_rsync_list(self):
        rsyncd =  RSYNCDownload('rsync', self.examples, "")
        rsyncd.set_credentials(None)
        #os.chdir(self.utils.data_dir)
        (files_list, dir_list) = rsyncd.list()
        self.assertTrue(len(files_list) != 0)
    
    def test_rsync_match(self):
        rsyncd =  RSYNCDownload('rsync', self.examples, "")
        rsyncd.set_credentials(None)
        #os.chdir(self.utils.data_dir)
        (files_list, dir_list) = rsyncd.list()
        rsyncd.match([r'^test.*\.gz$'], files_list, dir_list,prefix='', submatch=False)
        self.assertTrue(len(rsyncd.files_to_download) != 0)
    
    def test_rsync_download(self):
        rsyncd = RSYNCDownload('rsync', self.examples, "")
        rsyncd.set_credentials(None)
        #os.chdir(self.utils.data_dir)
        error = rsyncd.rsync_download(self.utils.data_dir, "test2.fasta")
        self.assertTrue(error == 0)
    
    
    def test_rsync_general_download(self):
        rsyncd =  RSYNCDownload('rsync', self.examples, "")
        rsyncd.set_credentials(None)
        #os.chdir(self.utils.data_dir)
        (files_list, dir_list) = rsyncd.list()
        rsyncd.match([r'^test.*\.gz$'],files_list,dir_list)
        error, download_files=rsyncd.download(self.curdir)
        self.assertTrue(error==0)
    
    def test_rsync_download_or_copy(self):
        rsyncd =  RSYNCDownload('rsync', self.examples, "")
        #os.chdir(self.utils.data_dir)
        (file_list, dir_list) = rsyncd.list()
        rsyncd.match([r'^test.*\.gz$'], file_list, dir_list)
        files_to_download_prev = rsyncd.files_to_download
        rsyncd.download_or_copy(rsyncd.files_to_download, self.examples, check_exists=True)
        self.assertTrue(files_to_download_prev != rsyncd.files_to_download)
