import pycurl
import re
import sys
from datetime import datetime
import hashlib

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


class FTPDownload(DownloadInterface):
    '''
    Base class to download files from FTP

    protocol=ftp
    server=ftp.ncbi.nih.gov
    remote.dir=/blast/db/FASTA/

    remote.files=^alu.*\\.gz$

    '''

    def __init__(self, protocol, host, rootdir):
        DownloadInterface.__init__(self)
        self.logger.debug('Download')
        self.crl = pycurl.Curl()
        self.protocol = protocol
        self.rootdir = rootdir
        self.url = protocol + '://' + host
        self.headers = {}

    def _append_file_to_download(self, rfile):
        # Add url and root to the file if needed (for safety)
        if 'url' not in rfile or not rfile['url']:
            rfile['url'] = self.url
        if 'root' not in rfile or not rfile['root']:
            rfile['root'] = self.rootdir
        super(FTPDownload, self)._append_file_to_download(rfile)

    def _file_url(self, rfile):
        # rfile['root'] is set to self.rootdir if needed but may be different.
        # We don't use os.path.join because rfile['name'] may starts with /
        return self.url + '/' + rfile['root'] + rfile['name']

    def _download(self, file_path, rfile):
        """
        This method is designed to work for FTP and HTTP.
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
                # 226 if for FTP and 200 is for HTTP
                if int(errcode) != 226 and int(errcode) != 200:
                    error = True
                    self.logger.error('Error while downloading ' + file_url + ' - ' + str(errcode))
                else:
                    error = False
            except Exception as e:
                self.logger.error('Could not get errcode:' + str(e))

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
        List FTP directory

        :return: tuple of file and dirs in current directory with details
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

    def chroot(self, cwd):
        self.logger.debug('Download: change dir ' + cwd)

    def close(self):
        if self.crl is not None:
            self.crl.close()
            self.crl = None
