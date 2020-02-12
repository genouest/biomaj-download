"""
Note that attributes 'network' and 'local_irods' are ignored for CI.

To run 'local_irods' tests, you need an iRODS server on localhost (default port,
user 'rods', password 'rods') and a zone /tempZone/home/rods. See
UtilsForLocalIRODSTest.
"""
from nose.plugins.attrib import attr

import json
import shutil
import os
import tempfile
import logging
import stat

from mock import patch

from irods.session import iRODSSession

from biomaj_core.config import BiomajConfig
from biomaj_core.utils import Utils
from biomaj_download.download.interface import DownloadInterface
from biomaj_download.download.curl import CurlDownload, HTTPParse
from biomaj_download.download.direct import DirectFTPDownload, DirectHTTPDownload
from biomaj_download.download.localcopy  import LocalDownload
from biomaj_download.download.rsync import RSYNCDownload
from biomaj_download.download.protocolirods import IRODSDownload

import unittest
import tenacity


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

    # Create an invalid archive file (empty file). This is deleted by clean().
    # See TestBiomajRSYNCDownload.test_rsync_download_skip_check_uncompress.
    self.invalid_archive = os.path.join(self.test_dir, 'invalid.gz')
    open(self.invalid_archive, 'w').close()

  def clean(self):
    """
    Deletes temp directory
    """
    shutil.rmtree(self.test_dir)

  def __copy_test_bank_properties(self):
    if self.bank_properties is not None:
      return
    # Copy bank configuration (those bank use external resources so there is no tuning to do)
    self.bank_properties = ['alu', 'testhttp', 'directhttp', 'multi']
    curdir = os.path.dirname(os.path.realpath(__file__))
    for b in self.bank_properties:
        from_file = os.path.join(curdir, b+'.properties')
        to_file = os.path.join(self.conf_dir, b+'.properties')
        shutil.copyfile(from_file, to_file)

    # Copy bank process
    self.bank_process = ['test.sh']
    curdir = os.path.dirname(os.path.realpath(__file__))
    procdir = os.path.join(curdir, 'bank/process')
    for proc in self.bank_process:
      from_file = os.path.join(procdir, proc)
      to_file = os.path.join(self.process_dir, proc)
      shutil.copyfile(from_file, to_file)
      os.chmod(to_file, stat.S_IRWXU)

    # Copy and adapt bank configuration that use local resources: we use the "bank" dir in current test directory as remote
    properties = ['local', 'localprocess', 'computed', 'computed2', 'sub1', 'sub2', 'computederror', 'error']
    for prop in properties:
      from_file = os.path.join(curdir, prop+'.properties')
      to_file = os.path.join(self.conf_dir, prop+'.properties')
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
    # Adapt directories in global configuration to the current test directory
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


class UtilsForLocalIRODSTest(UtilsForTest):
    """
    This class is used to prepare 'local_irods' tests.
    """
    SERVER = "localhost"
    PORT = 1247
    ZONE = "tempZone"
    USER = "rods"
    PASSWORD = "rods"
    COLLECTION = os.path.join("/" + ZONE, "home/rods/")  # Don't remove or add /

    def __init__(self):
        super(UtilsForLocalIRODSTest, self).__init__()
        self._session = iRODSSession(host=self.SERVER, port=self.PORT,
                                     user=self.USER, password=self.PASSWORD,
                                     zone=self.ZONE)
        self.curdir = os.path.dirname(os.path.realpath(__file__))
        # Copy some valid archives (bank/test.fasta.gz)
        file_ = os.path.join(self.curdir, "bank/test.fasta.gz")
        self._session.data_objects.put(file_, self.COLLECTION)
        # Copy invalid.gz
        self._session.data_objects.put(self.invalid_archive, self.COLLECTION)

    def clean(self):
        super(UtilsForLocalIRODSTest, self).clean()
        # Remove files on iRODS (use force otherwise the files are put in trash)
        # Remove test.fasta.gz
        self._session.data_objects.unlink(os.path.join(self.COLLECTION, "test.fasta.gz"), force=True)
        # Remove invalid.gz
        self._session.data_objects.unlink(os.path.join(self.COLLECTION, "invalid.gz"), force=True)


class TestDownloadInterface(unittest.TestCase):
  """
  Test of the interface.
  """

  def test_retry_parsing(self):
    """
    Test parsing of stop and wait conditions.
    """
    downloader = DownloadInterface()
    # Test some garbage
    d = dict(stop_condition="stop_after_attempts")  # no param
    self.assertRaises(ValueError, downloader.set_options, d)
    d = dict(stop_condition="1 & 1")  # not a stop_condition
    self.assertRaises(ValueError, downloader.set_options, d)
    d = dict(stop_condition="stop_after_attempts(5) & 1")  # not a stop_condition
    self.assertRaises(ValueError, downloader.set_options, d)
    # Test some garbage
    d = dict(wait_policy="wait_random")  # no param
    self.assertRaises(ValueError, downloader.set_options, d)
    d = dict(wait_policy="I love python")  # not a wait_condition
    self.assertRaises(ValueError, downloader.set_options, d)
    d = dict(wait_policy="wait_random(5) + 3")  # not a wait_condition
    self.assertRaises(ValueError, downloader.set_options, d)
    # Test operators
    d = dict(stop_condition="stop_never | stop_after_attempt(5)",
             wait_policy="wait_none + wait_random(1, 2)")
    downloader.set_options(d)
    # Test wait_combine, wait_chain
    d = dict(wait_policy="wait_combine(wait_fixed(3), wait_random(1, 2))")
    downloader.set_options(d)
    d = dict(wait_policy="wait_chain(wait_fixed(3), wait_random(1, 2))")
    downloader.set_options(d)
    # Test stop_any and stop_all
    stop_condition = "stop_any(stop_after_attempt(5), stop_after_delay(10))"
    d = dict(stop_condition=stop_condition)
    downloader.set_options(d)
    stop_condition = "stop_all(stop_after_attempt(5), stop_after_delay(10))"
    d = dict(stop_condition=stop_condition)
    downloader.set_options(d)


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

  def test_local_download_hardlinks(self):
    """
    Test download with hardlinks: we download a file from conf/ to data_dir.
    This should work unless /tmp don't accept hardlinks so the last assert is
    optional.
    """
    test_file = "conf/global.properties"
    locald = LocalDownload(self.utils.test_dir, use_hardlinks=True)
    (file_list, dir_list) = locald.list()
    locald.match([r'^/' + test_file + '$'], file_list, dir_list)
    locald.download(self.utils.data_dir)
    locald.close()
    self.assertTrue(len(locald.files_to_download) == 1)
    # Test if data/conf/global.properties is a hard link to
    # conf/global.properties
    local_global_properties = os.path.join(self.utils.test_dir, test_file)
    copy_global_properties = os.path.join(self.utils.data_dir, test_file)
    try:
      self.assertTrue(
        os.path.samefile(local_global_properties, copy_global_properties)
      )
    except Exception:
      msg = "In %s: copy worked but hardlinks were not used." % self.id()
      logging.info(msg)


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
    httpd = CurlDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_http_list_dateregexp(self):
    #self.http_parse.file_date_format = "%%d-%%b-%%Y %%H:%%M"
    self.http_parse.file_date_format = "%%Y-%%m-%%d %%H:%%M"
    httpd = CurlDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_http_download_no_size(self):
    self.http_parse = HTTPParse(self.config.get('http.parse.dir.line'),
        self.config.get('http.parse.file.line'),
        int(self.config.get('http.group.dir.name')),
        int(self.config.get('http.group.dir.date')),
        int(self.config.get('http.group.file.name')),
        int(self.config.get('http.group.file.date')),
        self.config.get('http.group.file.date_format', None),
        -1
    )
    self.http_parse.file_date_format = "%%Y-%%m-%%d %%H:%%M"
    httpd = CurlDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.match([r'^README$'], file_list, dir_list)
    httpd.download(self.utils.data_dir)
    httpd.close()
    self.assertTrue(len(httpd.files_to_download) == 1)

  def test_http_download_no_date(self):
    self.http_parse = HTTPParse(self.config.get('http.parse.dir.line'),
        self.config.get('http.parse.file.line'),
        int(self.config.get('http.group.dir.name')),
        int(self.config.get('http.group.dir.date')),
        int(self.config.get('http.group.file.name')),
        -1,
        self.config.get('http.group.file.date_format', None),
        int(self.config.get('http.group.file.size'))
    )
    httpd = CurlDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.match([r'^README$'], file_list, dir_list)
    httpd.download(self.utils.data_dir)
    httpd.close()
    self.assertTrue(len(httpd.files_to_download) == 1)

  def test_http_download(self):
    self.http_parse.file_date_format = "%%Y-%%m-%%d %%H:%%M"
    httpd = CurlDownload('http', 'ftp2.fr.debian.org', '/debian/dists/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    print(str(file_list))
    httpd.match([r'^README$'], file_list, dir_list)
    httpd.download(self.utils.data_dir)
    httpd.close()
    self.assertTrue(len(httpd.files_to_download) == 1)

  def test_http_download_in_subdir(self):
    self.http_parse.file_date_format = "%%Y-%%m-%%d %%H:%%M"
    httpd = CurlDownload('http', 'ftp2.fr.debian.org', '/debian/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.match([r'^dists/README$'], file_list, dir_list)
    httpd.download(self.utils.data_dir)
    httpd.close()
    self.assertTrue(len(httpd.files_to_download) == 1)


@attr('network')
@attr('https')
class TestBiomajHTTPSDownload(unittest.TestCase):
  """
  Test HTTPS downloader
  """

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()

  def test_download(self):
    self.utils = UtilsForTest()
    self.http_parse = HTTPParse(
        "<a[\s]+href=\"([\w\-\.]+\">[\w\-\.]+.tar.gz)<\/a>[\s]+([0-9]{2}-[A-Za-z]{3}-[0-9]{4}[\s][0-9]{2}:[0-9]{2})[\s]+([0-9]+[A-Za-z])",
        "<a[\s]+href=\"[\w\-\.]+\">([\w\-\.]+.tar.gz)<\/a>[\s]+([0-9]{2}-[A-Za-z]{3}-[0-9]{4}[\s][0-9]{2}:[0-9]{2})[\s]+([0-9]+[A-Za-z])",
        1,
        2,
        1,
        2,
        None,
        3
    )
    self.http_parse.file_date_format = "%%d-%%b-%%Y %%H:%%M"
    httpd = CurlDownload('https', 'mirrors.edge.kernel.org', '/pub/software/scm/git/debian/', self.http_parse)
    (file_list, dir_list) = httpd.list()
    httpd.match([r'^git-core-0.99.6.tar.gz$'], file_list, dir_list)
    httpd.download(self.utils.data_dir)
    httpd.close()
    self.assertTrue(len(httpd.files_to_download) == 1)


@attr('network')
@attr('sftp')
class TestBiomajSFTPDownload(unittest.TestCase):
  """
  Test SFTP downloader
  """

  PROTOCOL = "sftp"

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()

  def test_download(self):
    sftpd = CurlDownload(self.PROTOCOL, "test.rebex.net", "/")
    sftpd.set_credentials("demo:password")
    sftpd.set_options({
        "ssh_new_host": "add"
    })
    (file_list, dir_list) = sftpd.list()
    sftpd.match([r'^readme.txt$'], file_list, dir_list)
    sftpd.download(self.utils.data_dir)
    sftpd.close()
    self.assertTrue(len(sftpd.files_to_download) == 1)


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
    file_list = ['/debian/doc/mailing-lists.txt']
    ftpd = DirectFTPDownload('ftp', 'ftp.fr.debian.org', '')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_download(self):
    file_list = ['/debian/doc/mailing-lists.txt']
    ftpd = DirectFTPDownload('ftp', 'ftp.fr.debian.org', '')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'mailing-lists.txt')))


@attr('directftps')
@attr('network')
class TestBiomajDirectFTPSDownload(unittest.TestCase):
  """
  Test DirectFTP downloader with FTPS.
  """

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()

  def test_ftps_list(self):
    file_list = ['/readme.txt']
    ftpd = DirectFTPDownload('ftps', 'test.rebex.net', '')
    ftpd.set_credentials('demo:password')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_download(self):
    file_list = ['/readme.txt']
    ftpd = DirectFTPDownload('ftps', 'test.rebex.net', '')
    ftpd.set_credentials('demo:password')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'readme.txt')))


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
    ftpd = DirectHTTPDownload('http', 'ftp2.fr.debian.org', '')
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
    ftpd = DirectHTTPDownload('http', 'ftp2.fr.debian.org', '')
    ftpd.set_files_to_download(file_list)
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'README.html')))

  def test_download_get_params_save_as(self):
    file_list = ['/get']
    ftpd = DirectHTTPDownload('http', 'httpbin.org', '')
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
    ftpd = DirectHTTPDownload('http', 'ftp2.fr.debian.org', '')
    ftpd.set_files_to_download(file_list)
    ftpd.save_as = 'test.html'
    (file_list, dir_list) = ftpd.list()
    ftpd.download(self.utils.data_dir, False)
    ftpd.close()
    self.assertTrue(os.path.exists(os.path.join(self.utils.data_dir,'test.html')))

  def test_download_post_params(self):
    #file_list = ['/debian/README.html']
    file_list = ['/post']
    ftpd = DirectHTTPDownload('http', 'httpbin.org', '')
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
    ftpd = CurlDownload('ftp', 'speedtest.tele2.net', '/')
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) > 1)

  @attr('test')
  def test_download(self):
    ftpd = CurlDownload('ftp', 'speedtest.tele2.net', '/')
    (file_list, dir_list) = ftpd.list()
    ftpd.match([r'^1.*KB\.zip$'], file_list, dir_list)
    # This tests fails because the zip file is fake. We intercept the failure
    # and continue.
    # See test_download_skip_check_uncompress
    try:
        ftpd.download(self.utils.data_dir)
    except Exception:
        self.assertTrue(1==1)
    else:
        # In case it works, this is the real assertion
        self.assertTrue(len(ftpd.files_to_download) == 2)
    ftpd.close()

  def test_download_skip_check_uncompress(self):
    # This test is similar to test_download but we skip test of zip file.
    ftpd = CurlDownload('ftp', 'speedtest.tele2.net', '/')
    ftpd.set_options(dict(skip_check_uncompress=True))
    (file_list, dir_list) = ftpd.list()
    ftpd.match([r'^1.*KB\.zip$'], file_list, dir_list)
    ftpd.download(self.utils.data_dir)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download) == 2)

  def test_download_in_subdir(self):
    ftpd = CurlDownload('ftp', 'ftp.fr.debian.org', '/debian/')
    (file_list, dir_list) = ftpd.list()
    try:
        ftpd.match([r'^doc/mailing-lists.txt$'], file_list, dir_list)
    except Exception as e:
        print("Error: " + str(e))
        self.skipTest("Skipping test due to remote server error")
    ftpd.download(self.utils.data_dir)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download) == 1)

  def test_download_or_copy(self):
    ftpd = CurlDownload('ftp', 'ftp.fr.debian.org', '/debian/')
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

  def test_download_retry(self):
    """
    Try to download fake files to test retry.
    """
    n_attempts = 5
    ftpd = CurlDownload("ftp", "speedtest.tele2.net", "/")
    # Download a fake file
    ftpd.set_files_to_download([
          {'name': 'TOTO.zip', 'year': '2016', 'month': '02', 'day': '19',
           'size': 1, 'save_as': 'TOTO1KB'}
    ])
    ftpd.set_options(dict(stop_condition=tenacity.stop.stop_after_attempt(n_attempts),
                          wait_condition=tenacity.wait.wait_none()))
    self.assertRaisesRegexp(
        Exception, "^CurlDownload:Download:Error:",
        ftpd.download, self.utils.data_dir,
    )
    logging.debug(ftpd.retryer.statistics)
    self.assertTrue(len(ftpd.files_to_download) == 1)
    self.assertTrue(ftpd.retryer.statistics["attempt_number"] == n_attempts)
    # Try to download another file to ensure that it retryies
    ftpd.set_files_to_download([
          {'name': 'TITI.zip', 'year': '2016', 'month': '02', 'day': '19',
           'size': 1, 'save_as': 'TOTO1KB'}
    ])
    self.assertRaisesRegexp(
        Exception, "^CurlDownload:Download:Error:",
        ftpd.download, self.utils.data_dir,
    )
    self.assertTrue(len(ftpd.files_to_download) == 1)
    self.assertTrue(ftpd.retryer.statistics["attempt_number"] == n_attempts)
    ftpd.close()

  def test_ms_server(self):
      ftpd = CurlDownload("ftp", "test.rebex.net", "/")
      ftpd.set_credentials("demo:password")
      (file_list, dir_list) = ftpd.list()
      ftpd.match(["^readme.txt$"], file_list, dir_list)
      ftpd.download(self.utils.data_dir)
      ftpd.close()
      self.assertTrue(len(ftpd.files_to_download) == 1)

  def test_download_tcp_keepalive(self):
      """
      Test setting tcp_keepalive (it probably doesn't change anything here but
      we test that there is no obvious mistake in the code).
      """
      ftpd = CurlDownload("ftp", "test.rebex.net", "/")
      ftpd.set_options(dict(tcp_keepalive=10))
      ftpd.set_credentials("demo:password")
      (file_list, dir_list) = ftpd.list()
      ftpd.match(["^readme.txt$"], file_list, dir_list)
      ftpd.download(self.utils.data_dir)
      ftpd.close()
      self.assertTrue(len(ftpd.files_to_download) == 1)

  def test_download_ftp_method(self):
      """
      Test setting ftp_method (it probably doesn't change anything here but we
      test that there is no obvious mistake in the code).
      """
      ftpd = CurlDownload("ftp", "test.rebex.net", "/")
      ftpd.set_options(dict(ftp_method="nocwd"))
      ftpd.set_credentials("demo:password")
      (file_list, dir_list) = ftpd.list()
      ftpd.match(["^readme.txt$"], file_list, dir_list)
      ftpd.download(self.utils.data_dir)
      ftpd.close()
      self.assertTrue(len(ftpd.files_to_download) == 1)


@attr('ftps')
@attr('network')
class TestBiomajFTPSDownload(unittest.TestCase):
  """
  Test FTP downloader with FTPS.
  """
  PROTOCOL = "ftps"

  def setUp(self):
    self.utils = UtilsForTest()

  def tearDown(self):
    self.utils.clean()

  def test_ftps_list(self):
    ftpd = CurlDownload(self.PROTOCOL, "test.rebex.net", "/")
    ftpd.set_credentials("demo:password")
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) == 1)

  def test_download(self):
    ftpd = CurlDownload(self.PROTOCOL, "test.rebex.net", "/")
    ftpd.set_credentials("demo:password")
    (file_list, dir_list) = ftpd.list()
    ftpd.match([r'^readme.txt$'], file_list, dir_list)
    ftpd.download(self.utils.data_dir)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download) == 1)

  def test_ftps_list_no_ssl(self):
    # This server is misconfigured hence we disable all SSL verification
    SERVER = "demo.wftpserver.com"
    DIRECTORY = "/download/"
    CREDENTIALS = "demo-user:demo-user"
    ftpd = CurlDownload(self.PROTOCOL, SERVER, DIRECTORY)
    ftpd.set_options(dict(ssl_verifyhost="False", ssl_verifypeer="False"))
    ftpd.set_credentials(CREDENTIALS)
    (file_list, dir_list) = ftpd.list()
    ftpd.close()
    self.assertTrue(len(file_list) > 1)

  def test_download_no_ssl(self):
    # This server is misconfigured hence we disable all SSL verification
    SERVER = "demo.wftpserver.com"
    DIRECTORY = "/download/"
    CREDENTIALS = "demo-user:demo-user"
    ftpd = CurlDownload(self.PROTOCOL, SERVER, DIRECTORY)
    ftpd.set_options(dict(ssl_verifyhost="False", ssl_verifypeer="False"))
    ftpd.set_credentials(CREDENTIALS)
    (file_list, dir_list) = ftpd.list()
    ftpd.match([r'^manual_en.pdf$'], file_list, dir_list)
    ftpd.download(self.utils.data_dir)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download) == 1)

  def test_download_ssl_certficate(self):
    # This server is misconfigured but we use its certificate
    # The hostname is wrong so we disable host verification
    SERVER = "demo.wftpserver.com"
    DIRECTORY = "/download/"
    CREDENTIALS = "demo-user:demo-user"
    ftpd = CurlDownload(self.PROTOCOL, SERVER, DIRECTORY)
    curdir = os.path.dirname(os.path.realpath(__file__))
    cert_file = os.path.join(curdir, "caert.demo.wftpserver.com.pem")
    ftpd.set_options(dict(ssl_verifyhost="False", ssl_server_cert=cert_file))
    ftpd.set_credentials(CREDENTIALS)
    (file_list, dir_list) = ftpd.list()
    ftpd.match([r'^manual_en.pdf$'], file_list, dir_list)
    ftpd.download(self.utils.data_dir)
    ftpd.close()
    self.assertTrue(len(ftpd.files_to_download) == 1)


@attr('rsync')
@attr('local')
class TestBiomajRSYNCDownload(unittest.TestCase):
    '''
    Test RSYNC downloader
    '''
    def setUp(self):
        self.utils = UtilsForTest()

        self.curdir = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.examples = os.path.join(self.curdir,'bank') + '/'
        BiomajConfig.load_config(self.utils.global_properties, allow_user_config=False)

    def tearDown(self):
        self.utils.clean()

    def test_rsync_list(self):
        rsyncd = RSYNCDownload(self.examples, "")
        (files_list, dir_list) = rsyncd.list()
        self.assertTrue(len(files_list) != 0)

    def test_rsync_match(self):
        rsyncd = RSYNCDownload(self.examples, "")
        (files_list, dir_list) = rsyncd.list()
        rsyncd.match([r'^test.*\.gz$'], files_list, dir_list, prefix='', submatch=False)
        self.assertTrue(len(rsyncd.files_to_download) != 0)

    def test_rsync_download(self):
        rsyncd = RSYNCDownload(self.examples, "")
        rfile = {
            "name": "test2.fasta",
            "root": self.examples
        }
        error = rsyncd._download(self.utils.data_dir, rfile)
        self.assertFalse(error)

    def test_rsync_general_download(self):
        rsyncd = RSYNCDownload(self.examples, "")
        (files_list, dir_list) = rsyncd.list()
        rsyncd.match([r'^test.*\.gz$'],files_list,dir_list, prefix='')
        download_files=rsyncd.download(self.curdir)
        self.assertTrue(len(download_files)==1)

    def test_rsync_download_or_copy(self):
        rsyncd = RSYNCDownload(self.examples, "")
        (file_list, dir_list) = rsyncd.list()
        rsyncd.match([r'^test.*\.gz$'], file_list, dir_list, prefix='')
        files_to_download_prev = rsyncd.files_to_download
        rsyncd.download_or_copy(rsyncd.files_to_download, self.examples, check_exists=True)
        self.assertTrue(files_to_download_prev != rsyncd.files_to_download)

    def test_rsync_download_in_subdir(self):
        rsyncd = RSYNCDownload(self.curdir, "")
        (file_list, dir_list) = rsyncd.list()
        rsyncd.match([r'^/bank/test*'], file_list, dir_list, prefix='')
        rsyncd.download(self.utils.data_dir)
        self.assertTrue(len(rsyncd.files_to_download) == 3)

    def test_rsync_download_skip_check_uncompress(self):
        """
        Download the fake archive file with RSYNC but skip check.
        """
        rsyncd = RSYNCDownload(self.utils.test_dir + '/', "")
        rsyncd.set_options(dict(skip_check_uncompress=True))
        (file_list, dir_list) = rsyncd.list()
        rsyncd.match([r'invalid.gz'], file_list, dir_list, prefix='')
        rsyncd.download(self.utils.data_dir)
        self.assertTrue(len(rsyncd.files_to_download) == 1)

    def test_rsync_download_retry(self):
        """
        Try to download fake files to test retry.
        """
        n_attempts = 5
        rsyncd = RSYNCDownload(self.utils.test_dir + '/', "")
        rsyncd.set_options(dict(skip_check_uncompress=True))
        # Download a fake file
        rsyncd.set_files_to_download([
              {'name': 'TOTO.zip', 'year': '2016', 'month': '02', 'day': '19',
               'size': 1, 'save_as': 'TOTO1KB'}
        ])
        rsyncd.set_options(dict(stop_condition=tenacity.stop.stop_after_attempt(n_attempts),
                                wait_condition=tenacity.wait.wait_none()))
        self.assertRaisesRegexp(
            Exception, "^RSYNCDownload:Download:Error:",
            rsyncd.download, self.utils.data_dir,
        )
        logging.debug(rsyncd.retryer.statistics)
        self.assertTrue(len(rsyncd.files_to_download) == 1)
        self.assertTrue(rsyncd.retryer.statistics["attempt_number"] == n_attempts)
        # Try to download another file to ensure that it retryies
        rsyncd.set_files_to_download([
              {'name': 'TITI.zip', 'year': '2016', 'month': '02', 'day': '19',
               'size': 1, 'save_as': 'TOTO1KB'}
        ])
        self.assertRaisesRegexp(
            Exception, "^RSYNCDownload:Download:Error:",
            rsyncd.download, self.utils.data_dir,
        )
        self.assertTrue(len(rsyncd.files_to_download) == 1)
        self.assertTrue(rsyncd.retryer.statistics["attempt_number"] == n_attempts)
        rsyncd.close()


class iRodsResult(object):

    def __init__(self, collname, dataname, datasize, owner, modify):
        self.Collname = 'tests/'
        self.Dataname = 'test.fasta.gz'
        self.Datasize = 45
        self.Dataowner_name = 'biomaj'
        self.Datamodify_time = '2017-04-10 00:00:00'

    def __getitem__(self, index):
        from irods.models import Collection, DataObject, User
        if index.icat_id == DataObject.modify_time.icat_id:
            return self.Datamodify_time
        elif "DATA_SIZE" in str(index):
            return self.Datasize
        elif "DATA_NAME" in str(index):
            return 'test.fasta.gz'
        elif "COLL_NAME" in str(index):
            return self.Collname
        elif "D_OWNER_NAME" in str(index):
            return self.Dataowner_name


class MockiRODSSession(object):
    '''
    Simulation of python irods client
    for result in session.query(Collection.name, DataObject.name, DataObject.size, DataObject.owner_name, DataObject.modify_time).filter(User.name == self.user).get_results():
    '''
    def __init__(self):
       self.Collname="1"
       self.Dataname="2"
       self.Datasize="3"
       self.Dataowner_name="4"
       self.Datamodify_time="5"
       self.Collid=""

    def __getitem__(self, index):
        from irods.data_object import iRODSDataObject
        from irods.models import Collection, DataObject, User
        print(index)
        if "COLL_ID" in str(index):
            return self.Collid
        if "COLL_NAME" in str(index):
            return self.Collname

    def configure(self):
        return MockiRODSSession()

    def query(self,Collname, Dataname, Datasize, Dataowner_name, Datamodify_time):
        return self

    def all(self):
        return self

    def one(self):
        return self

    def filter(self,boo):
        return self

    def get_results(self):
        get_result_dict= iRodsResult('tests/', 'test.fasta.gz', 45, 'biomaj', '2017-04-10 00:00:00')
        return [get_result_dict]

    def cleanup(self):
        return self

    def open(self,r):
        my_test_file = open("tests/test.fasta.gz", "r+")
        return(my_test_file)


@attr('irods')
@attr('roscoZone')
@attr('network')
class TestBiomajIRODSDownload(unittest.TestCase):
    '''
    Test IRODS downloader
    '''
    def setUp(self):
        self.utils = UtilsForTest()
        self.curdir = os.path.dirname(os.path.realpath(__file__))
        self.examples = os.path.join(self.curdir,'bank') + '/'
        BiomajConfig.load_config(self.utils.global_properties, allow_user_config=False)

    def tearDown(self):
        self.utils.clean()

    @patch('irods.session.iRODSSession.configure')
    @patch('irods.session.iRODSSession.query')
    @patch('irods.session.iRODSSession.cleanup')
    def test_irods_list(self,initialize_mock, query_mock,cleanup_mock):
        mock_session=MockiRODSSession()
        initialize_mock.return_value=mock_session.configure()
        query_mock.return_value = mock_session.query(None,None,None,None,None)
        cleanup_mock.return_value = mock_session.cleanup()
        irodsd = IRODSDownload(self.examples, "")
        (files_list, dir_list) = irodsd.list()
        self.assertTrue(len(files_list) != 0)


@attr('local_irods')
@attr('network')
class TestBiomajLocalIRODSDownload(unittest.TestCase):
    """
    Test with a local iRODS server.
    """

    def setUp(self):
        self.utils = UtilsForLocalIRODSTest()
        self.curdir = os.path.dirname(os.path.realpath(__file__))
        self.examples = os.path.join(self.curdir,'bank') + '/'
        BiomajConfig.load_config(self.utils.global_properties, allow_user_config=False)

    def tearDown(self):
        self.utils.clean()

    def test_irods_download(self):
        irodsd = IRODSDownload(self.utils.SERVER, self.utils.COLLECTION)
        irodsd.set_param(dict(
            user=self.utils.USER,
            password=self.utils.PASSWORD,
        ))
        (file_list, dir_list) = irodsd.list()
        irodsd.match([r'^test.*\.gz$'], file_list, dir_list, prefix='')
        irodsd.download(self.utils.data_dir)
        self.assertTrue(len(irodsd.files_to_download) == 1)

    def test_irods_download_skip_check_uncompress(self):
        """
        Download the fake archive file with iRODS but skip check.
        """
        irodsd = IRODSDownload(self.utils.SERVER, self.utils.COLLECTION)
        irodsd.set_options(dict(skip_check_uncompress=True))
        irodsd.set_param(dict(
            user=self.utils.USER,
            password=self.utils.PASSWORD,
        ))
        (file_list, dir_list) = irodsd.list()
        irodsd.match([r'invalid.gz$'], file_list, dir_list, prefix='')
        irodsd.download(self.utils.data_dir)
        self.assertTrue(len(irodsd.files_to_download) == 1)

    def test_irods_download_retry(self):
        """
        Try to download fake files to test retry.
        """
        n_attempts = 5
        irodsd = IRODSDownload(self.utils.SERVER, self.utils.COLLECTION)
        irodsd.set_options(dict(skip_check_uncompress=True))
        irodsd.set_param(dict(
            user=self.utils.USER,
            password=self.utils.PASSWORD,
        ))
        # Download a fake file
        irodsd.set_files_to_download([
              {'name': 'TOTO.zip', 'year': '2016', 'month': '02', 'day': '19',
               'size': 1, 'save_as': 'TOTO1KB'}
        ])
        irodsd.set_options(dict(stop_condition=tenacity.stop.stop_after_attempt(n_attempts),
                                wait_condition=tenacity.wait.wait_none()))
        self.assertRaisesRegexp(
            Exception, "^IRODSDownload:Download:Error:",
            irodsd.download, self.utils.data_dir,
        )
        logging.debug(irodsd.retryer.statistics)
        self.assertTrue(len(irodsd.files_to_download) == 1)
        self.assertTrue(irodsd.retryer.statistics["attempt_number"] == n_attempts)
        # Try to download another file to ensure that it retryies
        irodsd.set_files_to_download([
              {'name': 'TITI.zip', 'year': '2016', 'month': '02', 'day': '19',
               'size': 1, 'save_as': 'TOTO1KB'}
        ])
        self.assertRaisesRegexp(
            Exception, "^IRODSDownload:Download:Error:",
            irodsd.download, self.utils.data_dir,
        )
        self.assertTrue(len(irodsd.files_to_download) == 1)
        self.assertTrue(irodsd.retryer.statistics["attempt_number"] == n_attempts)
        irodsd.close()
