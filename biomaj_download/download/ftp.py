import pycurl
import re
import os
import time
from datetime import datetime
import stat
import hashlib
import ftputil

from biomaj_core.utils import Utils
from biomaj_download.download.interface import DownloadInterface

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO

# We use stat.filemode to convert from mode octal value to string.
# In python < 3.3, stat.filmode is not defined.
# This code is copied from the current implementation of stat.filemode.
if 'filemode' not in stat.__dict__:
    _filemode_table = (
        ((stat.S_IFLNK,                "l"),    # noqa: E241
         (stat.S_IFREG,                "-"),    # noqa: E241
         (stat.S_IFBLK,                "b"),    # noqa: E241
         (stat.S_IFDIR,                "d"),    # noqa: E241
         (stat.S_IFCHR,                "c"),    # noqa: E241
         (stat.S_IFIFO,                "p")),   # noqa: E241
        ((stat.S_IRUSR,                "r"),),  # noqa: E241
        ((stat.S_IWUSR,                "w"),),  # noqa: E241
        ((stat.S_IXUSR | stat.S_ISUID, "s"),    # noqa: E241
         (stat.S_ISUID,                "S"),    # noqa: E241
         (stat.S_IXUSR,                "x")),   # noqa: E241
        ((stat.S_IRGRP,                "r"),),  # noqa: E241
        ((stat.S_IWGRP,                "w"),),  # noqa: E241
        ((stat.S_IXGRP | stat.S_ISGID, "s"),    # noqa: E241
         (stat.S_ISGID,                "S"),    # noqa: E241
         (stat.S_IXGRP,                "x")),   # noqa: E241
        ((stat.S_IROTH,                "r"),),  # noqa: E241
        ((stat.S_IWOTH,                "w"),),  # noqa: E241
        ((stat.S_IXOTH | stat.S_ISVTX, "t"),    # noqa: E241
         (stat.S_ISVTX,                "T"),    # noqa: E241
         (stat.S_IXOTH,                "x"))    # noqa: E241
    )

    def _filemode(mode):
        """Convert a file's mode to a string of the form '-rwxrwxrwx'."""
        perm = []
        for table in _filemode_table:
            for bit, char in table:
                if mode & bit == bit:
                    perm.append(char)
                    break
            else:
                perm.append("-")
        return "".join(perm)

    stat.filemode = _filemode


class FTPDownload(DownloadInterface):
    '''
    Base class to download files from FTP

    protocol=ftp
    server=ftp.ncbi.nih.gov
    remote.dir=/blast/db/FASTA/

    remote.files=^alu.*\\.gz$

    '''
    # Utilities to parse ftp listings: UnixParser is the more common hence we
    # put it first
    ftp_listing_parsers = [
        ftputil.stat.UnixParser(),
        ftputil.stat.MSParser(),
    ]

    def __init__(self, protocol, host, rootdir):
        DownloadInterface.__init__(self)
        self.logger.debug('Download')
        self.crl = pycurl.Curl()
        url = protocol + '://' + host
        self.rootdir = rootdir
        self.url = url
        self.headers = {}
        # Initialize options
        # Should we skip SSL verification (cURL -k/--insecure option)
        self.ssl_verifyhost = True
        self.ssl_verifypeer = True
        # Path to the certificate of the server (cURL --cacert option; PEM format)
        self.ssl_server_cert = None
        # Keep alive
        self.tcp_keepalive = 0

    def set_options(self, protocol_options):
        super(FTPDownload, self).set_options(protocol_options)
        if "ssl_verifyhost" in protocol_options:
            self.ssl_verifyhost = Utils.to_bool(protocol_options["ssl_verifyhost"])
        if "ssl_verifypeer" in protocol_options:
            self.ssl_verifypeer = Utils.to_bool(protocol_options["ssl_verifypeer"])
        if "ssl_server_cert" in protocol_options:
            self.ssl_server_cert = protocol_options["ssl_server_cert"]
        if "tcp_keepalive" in protocol_options:
            self.tcp_keepalive = Utils.to_int(protocol_options["tcp_keepalive"])

    def match(self, patterns, file_list, dir_list=None, prefix='', submatch=False):
        '''
        Find files matching patterns. Sets instance variable files_to_download.

        :param patterns: regexps to match
        :type patterns: list
        :param file_list: list of files to match
        :type file_list: list
        :param dir_list: sub directories in current dir
        :type dir_list: list
        :param prefix: directory prefix
        :type prefix: str
        :param submatch: first call to match, or called from match
        :type submatch: bool
        '''
        self.logger.debug('Download:File:RegExp:' + str(patterns))
        if dir_list is None:
            dir_list = []
        if not submatch:
            self.files_to_download = []
        for pattern in patterns:
            subdirs_pattern = pattern.split('/')
            if len(subdirs_pattern) > 1:
                # Pattern contains sub directories
                subdir = subdirs_pattern[0]
                if subdir == '^':
                    subdirs_pattern = subdirs_pattern[1:]
                    subdir = subdirs_pattern[0]
                # If getting all, get all files
                if pattern == '**/*':
                    for rfile in file_list:
                        rfile['root'] = self.rootdir
                        if prefix != '':
                            rfile['name'] = prefix + '/' + rfile['name']
                        self.files_to_download.append(rfile)
                        self.logger.debug('Download:File:MatchRegExp:' + rfile['name'])
                for direlt in dir_list:
                    subdir = direlt['name']
                    self.logger.debug('Download:File:Subdir:Check:' + subdir)
                    if pattern == '**/*':
                        (subfile_list, subdirs_list) = self.list(prefix + '/' + subdir + '/')
                        self.match([pattern], subfile_list, subdirs_list, prefix + '/' + subdir, True)

                    else:
                        if re.match(subdirs_pattern[0], subdir):
                            self.logger.debug('Download:File:Subdir:Match:' + subdir)
                            # subdir match the beginning of the pattern
                            # check match in subdir
                            (subfile_list, subdirs_list) = self.list(prefix + '/' + subdir + '/')
                            self.match(['/'.join(subdirs_pattern[1:])], subfile_list, subdirs_list, prefix + '/' + subdir, True)

            else:
                for rfile in file_list:
                    if re.match(pattern, rfile['name']):
                        rfile['root'] = self.rootdir
                        if prefix != '':
                            rfile['name'] = prefix + '/' + rfile['name']
                        self.files_to_download.append(rfile)
                        self.logger.debug('Download:File:MatchRegExp:' + rfile['name'])
        if not submatch and len(self.files_to_download) == 0:
            raise Exception('no file found matching expressions')

    def curl_download(self, file_path, file_to_download):
        error = True
        nbtry = 1
        while(error is True and nbtry < 3):
            fp = open(file_path, "wb")
            curl = pycurl.Curl()

            # Configure TCP keepalive
            if self.tcp_keepalive:
                curl.setopt(pycurl.TCP_KEEPALIVE, True)
                curl.setopt(pycurl.TCP_KEEPIDLE, self.tcp_keepalive * 2)
                curl.setopt(pycurl.TCP_KEEPINTVL, self.tcp_keepalive)

            # Configure SSL verification (on some platforms, disabling
            # SSL_VERIFYPEER implies disabling SSL_VERIFYHOST so we set
            # SSL_VERIFYPEER after)
            curl.setopt(pycurl.SSL_VERIFYHOST, 2 if self.ssl_verifyhost else 0)
            curl.setopt(pycurl.SSL_VERIFYPEER, 1 if self.ssl_verifypeer else 0)
            if self.ssl_server_cert:
                # cacert is the name of the option for the curl command. The
                # corresponding cURL option is CURLOPT_CAINFO.
                # See https://curl.haxx.se/libcurl/c/CURLOPT_CAINFO.html
                # This is inspired by that https://curl.haxx.se/docs/sslcerts.html
                # (section "Certificate Verification", option 2) but the option
                # CURLOPT_CAPATH is for a directory of certificates.
                curl.setopt(pycurl.CAINFO, self.ssl_server_cert)

            try:
                curl.setopt(pycurl.URL, file_to_download)
            except Exception:
                curl.setopt(pycurl.URL, file_to_download.encode('ascii', 'ignore'))
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

            try:
                curl.perform()
                errcode = curl.getinfo(pycurl.HTTP_CODE)
                if int(errcode) != 226 and int(errcode) != 200:
                    error = True
                    self.logger.error('Error while downloading ' + file_to_download + ' - ' + str(errcode))
                else:
                    error = False
            except Exception as e:
                self.logger.error('Could not get errcode:' + str(e))

            nbtry += 1
            curl.close()
            fp.close()
            if not error and not self.skip_check_uncompress:
                archive_status = Utils.archive_check(file_path)
                if not archive_status:
                    self.logger.error('Archive is invalid or corrupted, deleting file and retrying download')
                    error = True
                    if os.path.exists(file_path):
                        os.remove(file_path)
        return error

    def download(self, local_dir, keep_dirs=True):
        '''
        Download remote files to local_dir

        :param local_dir: Directory where files should be downloaded
        :type local_dir: str
        :param keep_dirs: keep file name directory structure or copy file in local_dir directly
        :param keep_dirs: bool
        :return: list of downloaded files
        '''
        self.logger.debug('FTP:Download')

        nb_files = len(self.files_to_download)
        cur_files = 1

        for rfile in self.files_to_download:
            if self.kill_received:
                raise Exception('Kill request received, exiting')
            file_dir = local_dir
            if 'save_as' not in rfile or not rfile['save_as']:
                rfile['save_as'] = rfile['name']
            if keep_dirs:
                file_dir = local_dir + '/' + os.path.dirname(rfile['save_as'])
            file_path = file_dir + '/' + os.path.basename(rfile['save_as'])

            # For unit tests only, workflow will take in charge directory creation before to avoid thread multi access
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            self.logger.debug('FTP:Download:Progress:' + str(cur_files) + '/' + str(nb_files) + ' downloading file ' + rfile['name'])
            self.logger.debug('FTP:Download:Progress:' + str(cur_files) + '/' + str(nb_files) + ' save as ' + rfile['save_as'])
            cur_files += 1
            if 'url' not in rfile or not rfile['url']:
                rfile['url'] = self.url
            if 'root' not in rfile or not rfile['root']:
                rfile['root'] = self.rootdir
            start_time = datetime.now()
            start_time = time.mktime(start_time.timetuple())
            error = self.curl_download(file_path, rfile['url'] + rfile['root'] + '/' + rfile['name'])
            if error:
                rfile['download_time'] = 0
                rfile['error'] = True
                raise Exception("FTP:Download:Error:" + rfile['url'] + rfile['root'] + '/' + rfile['name'])
            else:
                end_time = datetime.now()
                end_time = time.mktime(end_time.timetuple())
                rfile['download_time'] = end_time - start_time

            self.set_permissions(file_path, rfile)

        return self.files_to_download

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
        self.logger.debug('Download:List:' + self.url + self.rootdir + directory)

        # Configure TCP keepalive
        if self.tcp_keepalive:
            self.crl.setopt(pycurl.TCP_KEEPALIVE, True)
            self.crl.setopt(pycurl.TCP_KEEPIDLE, self.tcp_keepalive * 2)
            self.crl.setopt(pycurl.TCP_KEEPINTVL, self.tcp_keepalive)

        # See the corresponding lines in method:`curl_download`
        self.crl.setopt(pycurl.SSL_VERIFYHOST, 2 if self.ssl_verifyhost else 0)
        self.crl.setopt(pycurl.SSL_VERIFYPEER, 1 if self.ssl_verifypeer else 0)
        if self.ssl_server_cert:
            self.crl.setopt(pycurl.CAINFO, self.ssl_server_cert)

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

    def chroot(self, cwd):
        self.logger.debug('Download: change dir ' + cwd)

    def close(self):
        if self.crl is not None:
            self.crl.close()
            self.crl = None
