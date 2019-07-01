import sys
import os
import re
from datetime import datetime
import hashlib

import pycurl

import humanfriendly

from biomaj_core.utils import Utils
from biomaj_download.download.interface import DownloadInterface

if sys.version_info[0] < 3:
    from urllib import urlencode
else:
    from urllib.parse import urlencode

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO


class HTTPParse(object):

    def __init__(self, dir_line, file_line, dir_name=1, dir_date=2, file_name=1, file_date=2, file_date_format=None, file_size=3):
        r'''
        http.parse.dir.line: <img[\s]+src="[\S]+"[\s]+alt="\[DIR\]"[\s]*/?>[\s]*<a[\s]+href="([\S]+)/"[\s]*>.*([\d]{2}-[\w\d]{2,5}-[\d]{4}\s[\d]{2}:[\d]{2})
        http.parse.file.line: <img[\s]+src="[\S]+"[\s]+alt="\[[\s]+\]"[\s]*/?>[\s]<a[\s]+href="([\S]+)".*([\d]{2}-[\w\d]{2,5}-[\d]{4}\s[\d]{2}:[\d]{2})[\s]+([\d\.]+[MKG]{0,1})
        http.group.dir.name: 1
        http.group.dir.date: 2
        http.group.file.name: 1
        http.group.file.date: 2
        http.group.file.size: 3
        '''
        self.dir_line = dir_line
        self.file_line = file_line
        self.dir_name = dir_name
        self.dir_date = dir_date
        self.file_name = file_name
        self.file_date = file_date
        self.file_size = file_size
        self.file_date_format = file_date_format


class CurlDownload(DownloadInterface):
    '''
    Base class to download files from FTP, HTTP(S) and SFTP

    protocol=ftp
    server=ftp.ncbi.nih.gov
    remote.dir=/blast/db/FASTA/

    remote.files=^alu.*\\.gz$

    '''

    FTP_PROTOCOL_FAMILY = ["ftp", "ftps"]
    HTTP_PROTOCOL_FAMILY = ["http", "https"]
    SFTP_PROTOCOL_FAMILY = ["sftp"]
    ALL_PROTOCOLS = FTP_PROTOCOL_FAMILY + HTTP_PROTOCOL_FAMILY + SFTP_PROTOCOL_FAMILY

    def __init__(self, protocol, host, rootdir, http_parse=None):
        DownloadInterface.__init__(self)
        self.logger.debug('Download')
        self.crl = pycurl.Curl()
        protocol = protocol.lower()
        if protocol not in self.ALL_PROTOCOLS:
            raise ValueError("value must be one of %s (case insensitive)" % self.ALL_PROTOCOLS)
        self.protocol = protocol
        # Initialize protocol specific constants
        if self.protocol in self.FTP_PROTOCOL_FAMILY:
            self.protocol_family = "ftp"
            self._parse_result = self._ftp_parse_result
            self.ERRCODE_OK = 226
        if self.protocol in self.HTTP_PROTOCOL_FAMILY:
            self.protocol_family = "http"
            self._parse_result = self._http_parse_result
            self.ERRCODE_OK = 200
        if self.protocol in self.SFTP_PROTOCOL_FAMILY:
            self.protocol_family = "sftp"
            self._parse_result = self._ftp_parse_result
            self.ERRCODE_OK = 0
        self.host = host
        self.rootdir = rootdir
        self.url = self.protocol + '://' + self.host
        self.headers = {}
        self.http_parse = http_parse
        # Should we skip test of archives
        self.uncompress_skip_check = os.environ.get('UNCOMPRESS_SKIP_CHECK', False)
        # Should we skip host verification
        self.no_ssl_verifyhost = os.environ.get('NO_SSL_VERIFYHOST', False)

    def _append_file_to_download(self, rfile):
        # Add url and root to the file if needed (for safety)
        if 'url' not in rfile or not rfile['url']:
            rfile['url'] = self.url
        if 'root' not in rfile or not rfile['root']:
            rfile['root'] = self.rootdir
        super(CurlDownload, self)._append_file_to_download(rfile)

    def _file_url(self, rfile):
        # rfile['root'] is set to self.rootdir if needed but may be different.
        # We don't use os.path.join because rfile['name'] may starts with /
        return self.url + '/' + rfile['root'] + rfile['name']

    def _download(self, file_path, rfile):
        """
        This method is designed to work for FTP, HTTP(S) and SFTP.
        """
        error = True
        nbtry = 1
        # Forge URL of remote file
        file_url = self._file_url(rfile)
        while(error is True and nbtry < 3):
            fp = open(file_path, "wb")
            curl = pycurl.Curl()
            try:
                curl.setopt(pycurl.URL, file_url)
            except Exception:
                curl.setopt(pycurl.URL, file_url.encode('ascii', 'ignore'))
            if self.proxy is not None:
                curl.setopt(pycurl.PROXY, self.proxy)
                if self.proxy_auth is not None:
                    curl.setopt(pycurl.PROXYUSERPWD, self.proxy_auth)

            if self.credentials is not None:
                curl.setopt(pycurl.USERPWD, self.credentials)

            curl.setopt(pycurl.CONNECTTIMEOUT, 300)
            # Download should not take more than 5minutes
            curl.setopt(pycurl.TIMEOUT, self.timeout)
            curl.setopt(pycurl.NOSIGNAL, 1)
            curl.setopt(pycurl.WRITEDATA, fp)

            if self.no_ssl_verifyhost:
                curl.setopt(pycurl.SSL_VERIFYHOST, False)

            # This is specific to HTTP
            if self.method == 'POST':
                # Form data must be provided already urlencoded.
                postfields = urlencode(self.param)
                # Sets request method to POST,
                # Content-Type header to application/x-www-form-urlencoded
                # and data to send in request body.
                curl.setopt(pycurl.POSTFIELDS, postfields)

            try:
                curl.perform()
                errcode = curl.getinfo(pycurl.RESPONSE_CODE)
                if int(errcode) != self.ERRCODE_OK:
                    error = True
                    self.logger.error('Error while downloading ' + file_url + ' - ' + str(errcode))
                else:
                    error = False
            except Exception as e:
                self.logger.error('Could not get errcode:' + str(e))

            # Check that the archive is correct
            if (not error) and (not self.uncompress_skip_check):
                archive_status = Utils.archive_check(file_path)
                if not archive_status:
                    self.logger.error('Archive is invalid or corrupted, deleting file and retrying download')
                    error = True
                    if os.path.exists(file_path):
                        os.remove(file_path)
            nbtry += 1
            curl.close()
            fp.close()
        return error

    def header_function(self, header_line):
        # HTTP standard specifies that headers are encoded in iso-8859-1.
        # On Python 2, decoding step can be skipped.
        # On Python 3, decoding step is required.
        header_line = header_line.decode('iso-8859-1')

        # Header lines include the first status line (HTTP/1.x ...).
        # We are going to ignore all lines that don't have a colon in them.
        # This will botch headers that are split on multiple lines...
        if ':' not in header_line:
            return

        # Break the header line into header name and value.
        name, value = header_line.split(':', 1)

        # Remove whitespace that may be present.
        # Header lines include the trailing newline, and there may be whitespace
        # around the colon.
        name = name.strip()
        value = value.strip()

        # Header names are case insensitive.
        # Lowercase name here.
        name = name.lower()

        # Now we can actually record the header name and value.
        self.headers[name] = value

    def list(self, directory=''):
        '''
        List remote directory

        :return: tuple of file and dirs in current directory with details

        This is a generic method for HTTP and FTP. The protocol-specific parts
        are done in _<protocol>_parse_result.
        '''
        dir_url = self.url + self.rootdir + directory
        self.logger.debug('Download:List:' + dir_url)

        try:
            self.crl.setopt(pycurl.URL, dir_url)
        except Exception:
            self.crl.setopt(pycurl.URL, dir_url.encode('ascii', 'ignore'))

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

        if self.no_ssl_verifyhost:
            self.crl.setopt(pycurl.SSL_VERIFYHOST, False)

        self.crl.setopt(pycurl.CONNECTTIMEOUT, 300)
        # Download should not take more than 5minutes
        self.crl.setopt(pycurl.TIMEOUT, self.timeout)
        self.crl.setopt(pycurl.NOSIGNAL, 1)
        try:
            self.crl.perform()
        except Exception as e:
            self.logger.error('Could not get errcode:' + str(e))

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

        return self._parse_result(result)

    def _ftp_parse_result(self, result):
        # FTP LIST output is separated by \r\n
        # lets split the output in lines
        lines = re.split(r'[\n\r]+', result)
        # lets walk through each line
        rfiles = []
        rdirs = []

        for line in lines:
            rfile = {}
            # lets print each part separately
            parts = line.split()
            # the individual fields in this list of parts
            if not parts:
                continue
            rfile['permissions'] = parts[0]
            rfile['group'] = parts[2]
            rfile['user'] = parts[3]
            rfile['size'] = int(parts[4])
            rfile['month'] = Utils.month_to_num(parts[5])
            rfile['day'] = int(parts[6])
            rfile['hash'] = hashlib.md5(line.encode('utf-8')).hexdigest()
            try:
                rfile['year'] = int(parts[7])
            except Exception:
                # specific ftp case issues at getting date info
                curdate = datetime.now()
                rfile['year'] = curdate.year
                # Year not precised, month feater than current means previous year
                if rfile['month'] > curdate.month:
                    rfile['year'] = curdate.year - 1
                # Same month but later day => previous year
                if rfile['month'] == curdate.month and rfile['day'] > curdate.day:
                    rfile['year'] = curdate.year - 1
            rfile['name'] = parts[8]
            for i in range(9, len(parts)):
                if parts[i] == '->':
                    # Symlink, add to files AND dirs as we don't know the type of the link
                    rdirs.append(rfile)
                    break
                else:
                    rfile['name'] += ' ' + parts[i]

            is_dir = False
            if re.match('^d', rfile['permissions']):
                is_dir = True

            if not is_dir:
                rfiles.append(rfile)
            else:
                rdirs.append(rfile)
        return (rfiles, rdirs)

    def _http_parse_result(self, result):
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
                if self.http_parse.file_size != -1:
                    rfile['size'] = humanfriendly.parse_size(foundfile[self.http_parse.file_size - 1])
                else:
                    rfile['size'] = 0
                if self.http_parse.file_date != -1:
                    date = foundfile[self.http_parse.file_date - 1]
                    if self.http_parse.file_date_format:
                        date_object = datetime.strptime(date, self.http_parse.file_date_format.replace('%%', '%'))
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
                else:
                    today = datetime.now()
                    date = '%s-%s-%s' % (today.year, today.month, today.day)
                    rfile['month'] = today.month
                    rfile['day'] = today.day
                    rfile['year'] = today.year
                rfile['name'] = foundfile[self.http_parse.file_name - 1]
                filehash = (rfile['name'] + str(date) + str(rfile['size'])).encode('utf-8')
                rfile['hash'] = hashlib.md5(filehash).hexdigest()
                rfiles.append(rfile)
        return (rfiles, rdirs)

    def chroot(self, cwd):
        self.logger.debug('Download: change dir ' + cwd)

    def close(self):
        if self.crl is not None:
            self.crl.close()
            self.crl = None
