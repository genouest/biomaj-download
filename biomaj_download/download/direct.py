"""
Subclasses for direct download (i.e. downloading without regexp).

Methods match and list must be adapted.
"""
import datetime
import pycurl
import re
import hashlib
import sys

from biomaj_download.download.curl import CurlDownload
from biomaj_core.utils import Utils

if sys.version_info[0] < 3:
    from urllib import urlencode
else:
    from urllib.parse import urlencode

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO


class DirectFTPDownload(CurlDownload):
    '''
    download a list of files from FTP, no regexp
    '''

    ALL_PROTOCOLS = ["ftp", "ftps"]

    def __init__(self, curl_protocol, host, rootdir=''):
        CurlDownload.__init__(self, curl_protocol, host, rootdir)
        self.save_as = None

    def _append_file_to_download(self, file):
        '''
        Initialize the files in list with today as last-modification date.
        Size is also preset to zero, size will be set after download
        '''
        today = datetime.date.today()
        rfile = {}
        rfile['root'] = self.rootdir
        rfile['permissions'] = ''
        rfile['group'] = ''
        rfile['user'] = ''
        rfile['size'] = 0
        rfile['month'] = today.month
        rfile['day'] = today.day
        rfile['year'] = today.year
        if file.endswith('/'):
            rfile['name'] = file[:-1]
        else:
            rfile['name'] = file
        rfile['save_as'] = rfile['name']
        rfile['hash'] = None
        super(DirectFTPDownload, self)._append_file_to_download(rfile)

    def list(self, directory=''):
        '''
        FTP protocol does not give us the possibility to get file date from remote url
        '''
        # TODO: are we sure about this implementation ?
        return (self.files_to_download, [])

    def match(self, patterns, file_list, dir_list=None, prefix='', submatch=False):
        '''
        All files to download match, no pattern
        '''
        if dir_list is None:
            dir_list = []
        self.set_files_to_download(file_list)


class DirectHTTPDownload(DirectFTPDownload):

    ALL_PROTOCOLS = ["http", "https"]

    def __init__(self, curl_protocol, host, rootdir=''):
        DirectFTPDownload.__init__(self, curl_protocol, host, rootdir)
        self.method = 'GET'
        self.param = {}

    def _file_url(self, file_to_download):
        url = super(DirectHTTPDownload, self)._file_url(file_to_download)
        if self.method == "GET":
            url += '?' + urlencode(self.param)
        return url

    def download(self, local_dir, keep_dirs=True):
        '''
        Download remote files to local_dir

        :param local_dir: Directory where files should be downloaded
        :type local_dir: str
        :param keep_dirs: keep file name directory structure or copy file in local_dir directly
        :param keep_dirs: bool
        :return: list of downloaded files
        '''
        if len(self.files_to_download) > 1:
            self.files_to_download = []
            self.logger.error('DirectHTTP accepts only 1 file')
            # TODO: raise exception ?
        return super(DirectHTTPDownload, self).download(local_dir, keep_dirs)

    def list(self, directory=''):
        '''
        Try to get file headers to get last_modification and size
        '''
        file_url = self._file_url(self.files_to_download[0])
        for rfile in self.files_to_download:
            if self.save_as is None:
                self.save_as = rfile['name']

            rfile['save_as'] = self.save_as

            self.crl.setopt(pycurl.HEADER, True)
            if self.credentials is not None:
                self.crl.setopt(pycurl.USERPWD, self.credentials)

            if self.proxy is not None:
                self.crl.setopt(pycurl.PROXY, self.proxy)
                if self.proxy_auth is not None:
                    self.crl.setopt(pycurl.PROXYUSERPWD, self.proxy_auth)

            self.crl.setopt(pycurl.NOBODY, True)
            try:
                self.crl.setopt(pycurl.URL, file_url)
            except Exception:
                self.crl.setopt(pycurl.URL, file_url.encode('ascii', 'ignore'))

            output = BytesIO()
            # lets assign this buffer to pycurl object
            self.crl.setopt(pycurl.WRITEFUNCTION, output.write)
            self.crl.setopt(pycurl.HEADERFUNCTION, self.header_function)
            self.crl.perform()

            # Figure out what encoding was sent with the response, if any.
            # Check against lowercased header name.
            encoding = None
            if 'content-type' in self.headers:
                content_type = self.headers['content-type'].lower()
                match = re.search(r'charset=(\S+)', content_type)
                if match:
                    encoding = match.group(1)
            if encoding is None:
                # Default encoding for HTML is iso-8859-1.
                # Other content types may have different default encoding,
                # or in case of binary data, may have no encoding at all.
                encoding = 'iso-8859-1'

            # lets get the output in a string
            result = output.getvalue().decode(encoding)

            lines = re.split(r'[\n\r]+', result)
            for line in lines:
                parts = line.split(':')
                if parts[0].strip() == 'Content-Length':
                    rfile['size'] = int(parts[1].strip())
                if parts[0].strip() == 'Last-Modified':
                    # Sun, 06 Nov 1994
                    res = re.match(r'(\w+),\s+(\d+)\s+(\w+)\s+(\d+)', parts[1].strip())
                    if res:
                        rfile['hash'] = hashlib.md5(str(res.group(0)).encode('utf-8')).hexdigest()
                        rfile['day'] = int(res.group(2))
                        rfile['month'] = Utils.month_to_num(res.group(3))
                        rfile['year'] = int(res.group(4))
                        continue
                    # Sunday, 06-Nov-94
                    res = re.match(r'(\w+),\s+(\d+)-(\w+)-(\d+)', parts[1].strip())
                    if res:
                        rfile['hash'] = hashlib.md5(str(res.group(0)).encode('utf-8')).hexdigest()
                        rfile['day'] = int(res.group(2))
                        rfile['month'] = Utils.month_to_num(res.group(3))
                        rfile['year'] = 2000 + int(res.group(4))
                        continue
                    # Sun Nov  6 08:49:37 1994
                    res = re.match(r'(\w+)\s+(\w+)\s+(\d+)\s+\d{2}:\d{2}:\d{2}\s+(\d+)', parts[1].strip())
                    if res:
                        rfile['hash'] = hashlib.md5(str(res.group(0)).encode('utf-8')).hexdigest()
                        rfile['day'] = int(res.group(3))
                        rfile['month'] = Utils.month_to_num(res.group(2))
                        rfile['year'] = int(res.group(4))
                        continue
        return (self.files_to_download, [])
