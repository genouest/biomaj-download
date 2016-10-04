import pycurl
import re
import hashlib
import datetime

import humanfriendly

from biomaj_core.utils import Utils
from biomaj_download.download.ftp import FTPDownload

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO


class HTTPParse(object):

    def __init__(self, dir_line, file_line, dir_name=1, dir_date=2, file_name=1, file_date=2, file_date_format=None, file_size=3):
        """
        http.parse.dir.line: <img[\s]+src="[\S]+"[\s]+alt="\[DIR\]"[\s]*/?>[\s]*<a[\s]+href="([\S]+)/"[\s]*>.*([\d]{2}-[\w\d]{2,5}-[\d]{4}\s[\d]{2}:[\d]{2})
        http.parse.file.line: <img[\s]+src="[\S]+"[\s]+alt="\[[\s]+\]"[\s]*/?>[\s]<a[\s]+href="([\S]+)".*([\d]{2}-[\w\d]{2,5}-[\d]{4}\s[\d]{2}:[\d]{2})[\s]+([\d\.]+[MKG]{0,1})
        http.group.dir.name: 1
        http.group.dir.date: 2
        http.group.file.name: 1
        http.group.file.date: 2
        http.group.file.size: 3
        """
        self.dir_line = dir_line
        self.file_line = file_line
        self.dir_name = dir_name
        self.dir_date = dir_date
        self.file_name = file_name
        self.file_date = file_date
        self.file_size = file_size
        self.file_date_format = file_date_format


class HTTPDownload(FTPDownload):
    '''
    Base class to download files from HTTP

    Makes use of http.parse.dir.line etc.. regexps to extract page information

    protocol=http
    server=ftp.ncbi.nih.gov
    remote.dir=/blast/db/FASTA/

    remote.files=^alu.*\\.gz$

    '''

    def __init__(self, protocol, host, rootdir, http_parse=None):
        FTPDownload.__init__(self, protocol, host, rootdir)
        self.http_parse = http_parse

    def list(self, directory=''):
        '''
        List FTP directory

        :return: tuple of file and dirs in current directory with details
        '''
        self.logger.debug('Download:List:' + self.url + self.rootdir + directory)

        try:
            self.crl.setopt(pycurl.URL, self.url + self.rootdir + directory)
        except Exception:
            self.crl.setopt(pycurl.URL, (self.url + self.rootdir + directory).encode('ascii', 'ignore'))

        if self.proxy is not None:
            self.crl.setopt(pycurl.PROXY, self.proxy)
            if self.proxy_auth is not None:
                self.crl.setopt(pycurl.PROXYUSERPWD, self.proxy_auth)

        if self.credentials is not None:
            self.crl.setopt(pycurl.USERPWD, self.credentials)

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
            match = re.search('charset=(\S+)', content_type)
            if match:
                encoding = match.group(1)
        if encoding is None:
            # Default encoding for HTML is iso-8859-1.
            # Other content types may have different default encoding,
            # or in case of binary data, may have no encoding at all.
            encoding = 'iso-8859-1'

        # lets get the output in a string
        result = output.getvalue().decode(encoding)
        '''
        'http.parse.dir.line': r'<a[\s]+href="([\S]+)/".*alt="\[DIR\]">.*([\d]{2}-[\w\d]{2,5}-[\d]{4}\s[\d]{2}:[\d]{2})',
        'http.parse.file.line': r'<a[\s]+href="([\S]+)".*([\d]{2}-[\w\d]{2,5}-[\d]{4}\s[\d]{2}:[\d]{2})[\s]+([\d\.]+[MKG]{0,1})',
        'http.group.dir.name': 1,
        'http.group.dir.date': 2,
        'http.group.file.name': 1,
        'http.group.file.date': 2,
        'http.group.file.size': 3,
        '''

        rfiles = []
        rdirs = []

        dirs = re.findall(self.http_parse.dir_line, result)
        if dirs is not None and len(dirs) > 0:
            for founddir in dirs:
                rfile = {}
                rfile['permissions'] = ''
                rfile['group'] = ''
                rfile['user'] = ''
                rfile['size'] = 0
                date = founddir[self.http_parse.dir_date - 1]
                dirdate = date.split()
                parts = dirdate[0].split('-')
                # 19-Jul-2014 13:02
                rfile['month'] = Utils.month_to_num(parts[1])
                rfile['day'] = int(parts[0])
                rfile['year'] = int(parts[2])
                rfile['name'] = founddir[self.http_parse.dir_name - 1]
                rdirs.append(rfile)

        files = re.findall(self.http_parse.file_line, result)
        if files is not None and len(files) > 0:
            for foundfile in files:
                rfile = {}
                rfile['permissions'] = ''
                rfile['group'] = ''
                rfile['user'] = ''
                rfile['size'] = humanfriendly.parse_size(foundfile[self.http_parse.file_size - 1])
                date = foundfile[self.http_parse.file_date - 1]
                if self.http_parse.file_date_format:
                    date_object = datetime.datetime.strptime(date, self.http_parse.file_date_format.replace('%%', '%'))
                    rfile['month'] = date_object.month
                    rfile['day'] = date_object.day
                    rfile['year'] = date_object.year
                else:
                    dirdate = date.split()
                    parts = dirdate[0].split('-')
                    # 19-Jul-2014 13:02
                    rfile['month'] = Utils.month_to_num(parts[1])
                    rfile['day'] = int(parts[0])
                    rfile['year'] = int(parts[2])
                rfile['name'] = foundfile[self.http_parse.file_name - 1]
                filehash = (rfile['name'] + str(date) + str(rfile['size'])).encode('utf-8')
                rfile['hash'] = hashlib.md5(filehash).hexdigest()
                rfiles.append(rfile)

        return (rfiles, rdirs)
