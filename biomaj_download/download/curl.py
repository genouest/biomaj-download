import re
from datetime import datetime
import hashlib
import time
import stat
from urllib.parse import urlencode
from io import BytesIO

import pycurl
import ftputil

import humanfriendly

from biomaj_core.utils import Utils
from biomaj_core.config import BiomajConfig

from biomaj_download.download.interface import DownloadInterface


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
    Base class to download files from FTP(S), HTTP(S) and SFTP.

    protocol=ftp
    server=ftp.ncbi.nih.gov
    remote.dir=/blast/db/FASTA/

    remote.files=^alu.*\\.gz$

    '''

    FTP_PROTOCOL_FAMILY = ["ftp", "ftps"]
    HTTP_PROTOCOL_FAMILY = ["http", "https"]
    SFTP_PROTOCOL_FAMILY = ["sftp"]
    ALL_PROTOCOLS = FTP_PROTOCOL_FAMILY + HTTP_PROTOCOL_FAMILY + SFTP_PROTOCOL_FAMILY

    # Utilities to parse ftp listings: UnixParser is the more common hence we
    # put it first
    ftp_listing_parsers = [
        ftputil.stat.UnixParser(),
        ftputil.stat.MSParser(),
    ]

    # Valid values for ftp_method options as string and int
    VALID_FTP_FILEMETHOD = {
        "default": pycurl.FTPMETHOD_DEFAULT,
        "multicwd": pycurl.FTPMETHOD_MULTICWD,
        "nocwd": pycurl.FTPMETHOD_NOCWD,
        "singlecwd": pycurl.FTPMETHOD_SINGLECWD,
    }

    # Valid values for ssh_new_host options as string and int
    VALID_SSH_NEW_HOST = {
        "reject": pycurl.KHSTAT_REJECT,
        "accept": pycurl.KHSTAT_FINE,
        "add": pycurl.KHSTAT_FINE_ADD_TO_FILE,
    }

    def __init__(self, curl_protocol, host, rootdir, http_parse=None):
        """
        Initialize a CurlDownloader.

        :param curl_protocol: (real) protocol to use
        :type curl_protocol: str (see :py:var:~CurlDownload.ALL_PROTOCOLS)

        :param host: server name
        :type host: str

        :param rootdir: base directory
        :type rootdir: str

        :param http_parse: object used to extract file information from HTML pages
        :type http_parse: py:class:HTTPParse.
        """
        DownloadInterface.__init__(self)
        self.logger.debug('Download')

        # Check for ssh support
        curl_opts_info = pycurl.version_info()
        curl_opts = []
        for opt in curl_opts_info:
            if isinstance(opt, tuple):
                for o in opt:
                    curl_opts.append(o)
            else:
                curl_opts.append(opt)
        if 'sftp' not in curl_opts:
            CurlDownload.ALL_PROTOCOLS = CurlDownload.FTP_PROTOCOL_FAMILY + CurlDownload.HTTP_PROTOCOL_FAMILY
            self.logger.warning("sftp not supported by curl: %s" % str(curl_opts_info))
        # Initialize curl_protocol.
        # Note that we don't change that field in set_protocol since this
        # method uses the protocol from the configuration file. It's not clear
        # what to do in this case.
        curl_protocol = curl_protocol.lower()
        if curl_protocol not in self.ALL_PROTOCOLS:
            raise ValueError("curl_protocol must be one of %s (case insensitive). Got %s." % (self.ALL_PROTOCOLS, curl_protocol))
        self.curl_protocol = curl_protocol
        # Initialize protocol specific constants
        if self.curl_protocol in self.FTP_PROTOCOL_FAMILY:
            self.protocol_family = "ftp"
            self._parse_result = self._ftp_parse_result
            self.ERRCODE_OK = [221, 226]
        elif self.curl_protocol in self.HTTP_PROTOCOL_FAMILY:
            self.protocol_family = "http"
            self._parse_result = self._http_parse_result
            self.ERRCODE_OK = [200]
        elif self.curl_protocol in self.SFTP_PROTOCOL_FAMILY:
            self.protocol_family = "sftp"
            self._parse_result = self._ftp_parse_result
            self.ERRCODE_OK = [0]
        else:  # Should not happen since we check before
            raise ValueError("Unknown protocol")
        self.rootdir = rootdir
        self.set_server(host)
        self.headers = {}
        self.http_parse = http_parse
        # Create the cURL object
        # This object is shared by all operations to use the cache.
        # Before using it, call method:`_basic_curl_configuration`.
        self.crl = pycurl.Curl()
        #
        # Initialize options
        #
        # Should we skip SSL verification (cURL -k/--insecure option)
        self.ssl_verifyhost = True
        self.ssl_verifypeer = True
        # Path to the certificate of the server (cURL --cacert option; PEM format)
        self.ssl_server_cert = None
        # Keep alive
        self.tcp_keepalive = 0
        # FTP method (cURL --ftp-method option)
        self.ftp_method = pycurl.FTPMETHOD_DEFAULT  # Use cURL default
        # TODO: Don't store default values in BiomajConfig.DEFAULTS for
        # ssh_hosts_file and ssh_new_hosts
        # known_hosts file
        self.ssh_hosts_file = BiomajConfig.DEFAULTS["ssh_hosts_file"]
        # How to treat unknown host
        self.ssh_new_host = self.VALID_SSH_NEW_HOST[BiomajConfig.DEFAULTS["ssh_new_host"]]
        # Allow redirections
        self.allow_redirections = True

    def _accept_new_hosts(self, known_key, found_key, match):
        # Key found in file: we can accept it
        # Don't use KHSTAT_FINE_ADD_TO_FILE because the key would be duplicated
        # See https://github.com/curl/curl/issues/4953.
        if match == pycurl.KHMATCH_OK:
            return pycurl.KHSTAT_FINE
        # Key not found in file: use the ssh_new_host option
        elif match == pycurl.KHMATCH_MISSING:
            return self.ssh_new_host
        # Key missmatch: the best option is to reject it
        else:
            return pycurl.KHSTAT_REJECT

    def _network_configuration(self):
        """
        Perform basic configuration (i.e. that doesn't depend on the
        operation: _download or list). This method should be called before any
        operation.
        """
        # Reset cURL options before setting them
        self.crl.reset()

        if self.proxy is not None:
            self.crl.setopt(pycurl.PROXY, self.proxy)
            if self.proxy_auth is not None:
                self.crl.setopt(pycurl.PROXYUSERPWD, self.proxy_auth)

        if self.credentials is not None:
            self.crl.setopt(pycurl.USERPWD, self.credentials)

        # Hosts file & function to decide for new hosts
        if self.curl_protocol in self.SFTP_PROTOCOL_FAMILY:
            self.crl.setopt(pycurl.SSH_KNOWNHOSTS, self.ssh_hosts_file)
            self.crl.setopt(pycurl.SSH_KEYFUNCTION, self._accept_new_hosts)

        # Configure TCP keepalive
        if self.tcp_keepalive:
            try:
                self.crl.setopt(pycurl.TCP_KEEPALIVE, True)
                self.crl.setopt(pycurl.TCP_KEEPIDLE, self.tcp_keepalive * 2)
                self.crl.setopt(pycurl.TCP_KEEPINTVL, self.tcp_keepalive)
            except Exception as e:
                self.logger.exception("TCP keepalive option failed: " + str(e))

        # Configure SSL verification (on some platforms, disabling
        # SSL_VERIFYPEER implies disabling SSL_VERIFYHOST so we set
        # SSL_VERIFYPEER after)
        self.crl.setopt(pycurl.SSL_VERIFYHOST, 2 if self.ssl_verifyhost else 0)
        self.crl.setopt(pycurl.SSL_VERIFYPEER, 1 if self.ssl_verifypeer else 0)
        if self.ssl_server_cert:
            # cacert is the name of the option for the curl command. The
            # corresponding cURL option is CURLOPT_CAINFO.
            # See https://curl.haxx.se/libcurl/c/CURLOPT_CAINFO.html
            # This is inspired by that https://curl.haxx.se/docs/sslcerts.html
            # (section "Certificate Verification", option 2) but the option
            # CURLOPT_CAPATH is for a directory of certificates.
            self.crl.setopt(pycurl.CAINFO, self.ssl_server_cert)

        # Configure ftp method
        self.crl.setopt(pycurl.FTP_FILEMETHOD, self.ftp_method)

        # Configure redirections
        self.crl.setopt(pycurl.FOLLOWLOCATION, self.allow_redirections)

        # Configure timeouts
        self.crl.setopt(pycurl.CONNECTTIMEOUT, 300)
        self.crl.setopt(pycurl.TIMEOUT, self.timeout)
        self.crl.setopt(pycurl.NOSIGNAL, 1)

        # Header function
        self.crl.setopt(pycurl.HEADERFUNCTION, self._header_function)

    def _header_function(self, header_line):
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

    def set_server(self, server):
        super(CurlDownload, self).set_server(server)
        self.url = self.curl_protocol + '://' + self.server

    def set_options(self, options):
        super(CurlDownload, self).set_options(options)
        if "ssl_verifyhost" in options:
            self.ssl_verifyhost = Utils.to_bool(options["ssl_verifyhost"])
        if "ssl_verifypeer" in options:
            self.ssl_verifypeer = Utils.to_bool(options["ssl_verifypeer"])
        if "ssl_server_cert" in options:
            self.ssl_server_cert = options["ssl_server_cert"]
        if "tcp_keepalive" in options:
            self.tcp_keepalive = Utils.to_int(options["tcp_keepalive"])
        if "ftp_method" in options:
            # raw_val is a string which contains the name of the option as in the CLI.
            # We always convert raw_val to a valid integer
            raw_val = options["ftp_method"].lower()
            if raw_val not in self.VALID_FTP_FILEMETHOD:
                raise ValueError("Invalid value for ftp_method")
            self.ftp_method = self.VALID_FTP_FILEMETHOD[raw_val]
        if "ssh_hosts_file" in options:
            self.ssh_hosts_file = options["ssh_hosts_file"]
        if "ssh_new_host" in options:
            raw_val = options["ssh_new_host"].lower()
            if raw_val not in self.VALID_SSH_NEW_HOST:
                raise ValueError("Invalid value for ssh_new_host")
            self.ssh_new_host = self.VALID_SSH_NEW_HOST[raw_val]
        if "allow_redirections" in options:
            self.allow_redirections = Utils.to_bool(options["allow_redirections"])

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
        url = self.url + '/' + rfile['root'] + rfile['name']
        url_elts = url.split('://')
        if len(url_elts) == 2:
            url_elts[1] = re.sub("/{2,}", "/", url_elts[1])
            return '://'.join(url_elts)
        return re.sub("/{2,}", "/", url)

    def _download(self, file_path, rfile):
        """
        Download one file and return False in case of success and True
        otherwise.

        This method is designed to work for FTP(S), HTTP(S) and SFTP.
        """
        error = True
        # Forge URL of remote file
        file_url = self._file_url(rfile)

        try:
            self.crl.setopt(pycurl.URL, file_url)
        except Exception:
            self.crl.setopt(pycurl.URL, file_url.encode('ascii', 'ignore'))

        # Create file and assign it to the pycurl object
        fp = open(file_path, "wb")
        self.crl.setopt(pycurl.WRITEFUNCTION, fp.write)

        # This is specific to HTTP
        if self.method == 'POST':
            # Form data must be provided already urlencoded.
            postfields = urlencode(self.param)
            # Sets request method to POST,
            # Content-Type header to application/x-www-form-urlencoded
            # and data to send in request body.
            self.crl.setopt(pycurl.POSTFIELDS, postfields)

        # Try download (we don't raise errors here since its the return value
        # ('error') that matters for the calling method; this is set to True
        # only in case of success).
        try:
            self.crl.perform()
            errcode = self.crl.getinfo(pycurl.RESPONSE_CODE)
            if int(errcode) not in self.ERRCODE_OK:
                error = True
                self.logger.error('Error while downloading ' + file_url + ' - ' + str(errcode))
            else:
                error = False
        except Exception as e:
            self.logger.error('Error while downloading ' + file_url + ' - ' + str(e))

        # Check if we were redirected
        if self.curl_protocol in self.HTTP_PROTOCOL_FAMILY:
            n_redirect = self.crl.getinfo(pycurl.REDIRECT_COUNT)
            if n_redirect:
                real_url = self.crl.getinfo(pycurl.EFFECTIVE_URL)
                redirect_time = self.crl.getinfo(pycurl.REDIRECT_TIME)
                msg_fmt = 'Download was redirected to %s (%i redirection(s), took %ss)'
                msg = msg_fmt % (real_url, n_redirect, redirect_time)
                self.logger.info(msg)

        # Close file
        fp.close()

        if error:
            return error

        # Our part is done so call parent _download
        return super(CurlDownload, self)._download(file_path, rfile)

    def list(self, directory=''):
        '''
        List remote directory

        :return: tuple of file and dirs in current directory with details

        This is a generic method for HTTP and FTP. The protocol-specific parts
        are done in _<protocol>_parse_result.
        '''
        dirbase = re.sub('//+', '/', self.rootdir + directory)
        dir_url = self.url + dirbase
        self.logger.debug('Download:List:' + dir_url)

        self._network_configuration()

        try:
            self.crl.setopt(pycurl.URL, dir_url)
        except Exception:
            self.crl.setopt(pycurl.URL, dir_url.encode('ascii', 'ignore'))

        # Create buffer and assign it to the pycurl object
        output = BytesIO()
        self.crl.setopt(pycurl.WRITEFUNCTION, output.write)

        # Try to list
        try:
            self.crl.perform()
            errcode = self.crl.getinfo(pycurl.RESPONSE_CODE)
            if int(errcode) not in self.ERRCODE_OK:
                msg = 'Error while listing ' + dir_url + ' - ' + str(errcode)
                self.logger.error(msg)
                raise Exception(msg)
        except Exception as e:
            msg = 'Error while listing ' + dir_url + ' - ' + str(e)
            self.logger.error(msg)
            raise e

        # Check if we were redirected
        if self.curl_protocol in self.HTTP_PROTOCOL_FAMILY:
            n_redirect = self.crl.getinfo(pycurl.REDIRECT_COUNT)
            if n_redirect:
                real_url = self.crl.getinfo(pycurl.EFFECTIVE_URL)
                redirect_time = self.crl.getinfo(pycurl.REDIRECT_TIME)
                msg_fmt = 'Download was redirected to %s (%i redirection(s), took %ss)'
                msg = msg_fmt % (real_url, n_redirect, redirect_time)
                self.logger.info(msg)

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
            # Skip empty lines (usually the last)
            if not line:
                continue
            # Parse the line
            for i, parser in enumerate(self.ftp_listing_parsers, 1):
                try:
                    stats = parser.parse_line(line)
                    break
                except ftputil.error.ParserError:
                    # If it's the last parser, re-raise the exception
                    if i == len(self.ftp_listing_parsers):
                        raise
                    else:
                        continue
            # Put stats in a dict
            rfile = {}
            rfile['name'] = stats._st_name
            # Reparse mode to a string
            rfile['permissions'] = stat.filemode(stats.st_mode)
            rfile['group'] = stats.st_gid
            rfile['user'] = stats.st_uid
            rfile['size'] = stats.st_size
            mtime = time.localtime(stats.st_mtime)
            rfile['year'] = mtime.tm_year
            rfile['month'] = mtime.tm_mon
            rfile['day'] = mtime.tm_mday
            rfile['hash'] = hashlib.md5(line.encode('utf-8')).hexdigest()

            is_link = stat.S_ISLNK(stats.st_mode)
            is_dir = stat.S_ISDIR(stats.st_mode)
            # Append links to dirs and files since we don't know what the
            # target is
            if is_link:
                rfiles.append(rfile)
                rdirs.append(rfile)
            else:
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

    def close(self):
        if self.crl is not None:
            self.crl.close()
            self.crl = None
