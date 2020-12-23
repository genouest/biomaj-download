"""
Subclasses for direct download (i.e. downloading without regexp). The usage is
a bit different: instead of calling method:`list` and method:`match`, client
code explicitly calls method:`set_files_to_download` (passing a list
containing only the file name). method:`list` is used to get more information
about the file (if possible). method:`match` matches everything.
Also client code can use method:`set_save_as` to indicate the name of the file
to save.

The trick for the implementation is to override
method:`_append_file_to_download` to initialize the rfile with the file name
and dummy values. Note that we use a list of rfile even if it contains only one
file.
method:`list` will modify directly the files_to_download.
method:``match` don't call method:`_append_file_to_download` (since the list of
files to download is already set up).
We also override method:`set_files_to_download` to check that we pass only one
file.
"""
import datetime
import pycurl
import re
import hashlib
import os
from urllib.parse import urlencode
from io import BytesIO

from biomaj_download.download.curl import CurlDownload
from biomaj_core.utils import Utils


class DirectFTPDownload(CurlDownload):
    '''
    download a list of files from FTP, no regexp
    '''

    ALL_PROTOCOLS = ["ftp", "ftps"]

    def _append_file_to_download(self, rfile):
        '''
        Initialize the files in list with today as last-modification date.
        Size is also preset to zero.
        '''
        filename = None
        # workaround to handle file dict info or file name
        # this is dirty, we expect to handle dicts now,
        # biomaj workflow should fix this
        if isinstance(rfile, dict):
            filename = rfile['name']
        else:
            # direct protocol send directly some filename
            filename = rfile
        today = datetime.date.today()
        new_rfile = {}
        new_rfile['root'] = self.rootdir
        new_rfile['permissions'] = ''
        new_rfile['group'] = ''
        new_rfile['user'] = ''
        new_rfile['size'] = 0
        new_rfile['month'] = today.month
        new_rfile['day'] = today.day
        new_rfile['year'] = today.year
        if filename.endswith('/'):
            new_rfile['name'] = filename[:-1]
        else:
            new_rfile['name'] = filename
        new_rfile['hash'] = None
        # Use self.save_as even if we use it in list(). This is important.
        new_rfile['save_as'] = self.save_as
        super(DirectFTPDownload, self)._append_file_to_download(new_rfile)

    def set_files_to_download(self, files_to_download):
        if len(files_to_download) > 1:
            self.files_to_download = []
            msg = self.__class__.__name__ + ' accepts only 1 file'
            self.logger.error(msg)
            raise ValueError(msg)
        return super(DirectFTPDownload, self).set_files_to_download(files_to_download)

    def _file_url(self, rfile):
        # rfile['root'] is set to self.rootdir if needed but may be different.
        # We don't use os.path.join because rfile['name'] may starts with /
        url = self.url + '/' + rfile['root'] + rfile['name']
        url_elts = url.split('://')
        url_elts[1] = re.sub("/{2,}", "/", url_elts[1])
        return '://'.join(url_elts)

    def list(self, directory=''):
        '''
        FTP protocol does not give us the possibility to get file date from remote url
        '''
        self._network_configuration()
        # Specific configuration
        # With those options, cURL will issue a sequence of commands (SIZE,
        # MDTM) to get the file size and last modification time and then issue
        # a REST command. This usually ends with code 350. Therefore we
        # explicitly handle this in this method.
        # Note that very old servers may not support the MDTM command.
        # Therefore, cURL will raise an error (although we can probably
        # download the file).
        self.crl.setopt(pycurl.OPT_FILETIME, True)
        self.crl.setopt(pycurl.NOBODY, True)
        for rfile in self.files_to_download:
            if self.save_as is None:
                self.save_as = os.path.basename(rfile['name'])
            rfile['save_as'] = self.save_as
            file_url = self._file_url(rfile)
            try:
                self.crl.setopt(pycurl.URL, file_url)
            except Exception:
                self.crl.setopt(pycurl.URL, file_url.encode('ascii', 'ignore'))
            self.crl.setopt(pycurl.URL, file_url)

            try:
                self.crl.perform()
                errcode = int(self.crl.getinfo(pycurl.RESPONSE_CODE))
                # As explained, 350 is correct. We check against ERRCODE_OK
                # just in case.
                if errcode != 350 and errcode not in self.ERRCODE_OK:
                    msg = 'Error while listing ' + file_url + ' - ' + str(errcode)
                    self.logger.error(msg)
                    raise Exception(msg)
            except Exception as e:
                msg = 'Error while listing ' + file_url + ' - ' + str(e)
                self.logger.error(msg)
                raise e

            timestamp = self.crl.getinfo(pycurl.INFO_FILETIME)
            dt = datetime.datetime.fromtimestamp(timestamp)
            size_file = int(self.crl.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD))

            rfile['year'] = dt.year
            rfile['month'] = dt.month
            rfile['day'] = dt.day
            rfile['size'] = size_file
            rfile['hash'] = hashlib.md5(str(timestamp).encode('utf-8')).hexdigest()
        return (self.files_to_download, [])

    def match(self, patterns, file_list, dir_list=None, prefix='', submatch=False):
        '''
        All files to download match, no pattern
        '''
        pass


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

    def list(self, directory=''):
        '''
        Try to get file headers to get last_modification and size
        '''
        self._network_configuration()
        # Specific configuration
        # With those options, cURL will issue a HEAD request. This may not be
        # supported especially on resources that are accessed using POST. In
        # this case, HTTP will return code 405. We explicitely handle this case
        # in this method.
        # Note also that in many cases, there is no Last-Modified field in
        # headers since this is usually dynamic content (Content-Length is
        # usually present).
        self.crl.setopt(pycurl.HEADER, True)
        self.crl.setopt(pycurl.NOBODY, True)
        for rfile in self.files_to_download:
            if self.save_as is None:
                self.save_as = rfile['name']

            rfile['save_as'] = self.save_as

            file_url = self._file_url(rfile)
            try:
                self.crl.setopt(pycurl.URL, file_url)
            except Exception:
                self.crl.setopt(pycurl.URL, file_url.encode('ascii', 'ignore'))

            # Create a buffer and assign it to the pycurl object
            output = BytesIO()
            self.crl.setopt(pycurl.WRITEFUNCTION, output.write)

            try:
                self.crl.perform()
                errcode = int(self.crl.getinfo(pycurl.RESPONSE_CODE))
                if errcode == 405:
                    # HEAD not supported by the server for this URL so we can
                    # skip the rest of the loop (we won't have metadata about
                    # the file but biomaj should be fine).
                    msg = 'Listing ' + file_url + ' not supported. This is fine, continuing.'
                    self.logger.info(msg)
                    continue
                elif errcode not in self.ERRCODE_OK:
                    msg = 'Error while listing ' + file_url + ' - ' + str(errcode)
                    self.logger.error(msg)
                    raise Exception(msg)
            except Exception as e:
                msg = 'Error while listing ' + file_url + ' - ' + str(e)
                self.logger.error(msg)
                raise e

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
