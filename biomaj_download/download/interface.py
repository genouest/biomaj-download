import os
import logging
import datetime
import time
import re

from biomaj_core.utils import Utils


class _FakeLock(object):
    '''
    Fake lock for downloaders not called by a Downloadthread
    '''

    def __init__(self):
        pass

    def acquire(self):
        pass

    def release(self):
        pass


class DownloadInterface(object):
    '''
    Main interface that all downloaders must extend.

    The methods are divided into 2 broad categories:
      - setters which act on properties of the downloader; those methods are
        important in microservice mode
      - file operations which are used to list and match remote files, download
        them, etc.

    Usually, it is enough to overload list, _append_file_to_download and
    _download.

    TODO:
      - the purpose of some setters (set_server, set_protocol) is not clear
        since a subclass cannot always change those parameters arbitrarily
      - chroot is not used in BioMaJ
    '''

    files_num_threads = 4

    def __init__(self):
        # This variable defines the protocol as passed by the config file (i.e.
        # this is directftp for DirectFTPDownload). It is used by the workflow
        # to send the download message so it must be set.
        self.protocol = None
        self.config = None
        self.files_to_download = []
        self.files_to_copy = []
        self.error = False
        self.credentials = None
        # bank name
        self.bank = None
        self.mkdir_lock = _FakeLock()
        self.kill_received = False
        self.proxy = None
        # 24h timeout
        self.timeout = 3600
        # Optional save target for single file downloaders
        self.save_as = None
        self.logger = logging.getLogger('biomaj')
        self.param = None
        self.method = None
        self.server = None
        self.offline_dir = None
        # Options
        self.options = {}  # This field is used to forge the download message
        self.skip_check_uncompress = False

    #
    # Setters for downloader
    #

    def set_offline_dir(self, offline_dir):
        self.offline_dir = offline_dir

    def set_server(self, server):
        self.server = server

    def set_protocol(self, protocol):
        """
        Method used by DownloadService to set the protocol. This value is
        passed from the config file so is not always a real protocol (for
        instance it can be "directhttp" for a direct downloader).
        """
        self.protocol = protocol

    def set_param(self, param):
        self.param = param

    def set_timeout(self, timeout):
        if isinstance(timeout, int):
            self.timeout = timeout
        else:
            try:
                self.timeout = int(timeout)
            except Exception:
                logging.error('Timeout is not a valid integer, skipping')

    def set_save_as(self, save_as):
        self.save_as = save_as

    def set_proxy(self, proxy, proxy_auth=None):
        '''
        Use a proxy to connect to remote servers

        :param proxy: proxy to use (see http://curl.haxx.se/libcurl/c/CURLOPT_PROXY.html for format)
        :type proxy: str
        :param proxy_auth: proxy authentication if any (user:password)
        :type proxy_auth: str
        '''
        self.proxy = proxy
        self.proxy_auth = proxy_auth

    def set_method(self, method):
        self.method = method

    def set_credentials(self, userpwd):
        '''
        Set credentials in format user:pwd

        :param userpwd: credentials
        :type userpwd: str
        '''
        self.credentials = userpwd

    def set_options(self, options):
        """
        Set download options.

        Subclasses that override this method must call this implementation.
        """
        # Copy the option dict
        self.options = options
        if "skip_check_uncompress" in options:
            self.skip_check_uncompress = Utils.to_bool(options["skip_check_uncompress"])

    #
    # File operations (match, list, download) and associated hook methods
    #

    def _append_file_to_download(self, rfile):
        """
        Add a file to the download list and check its properties (this method
        is called in `match` and `set_files_to_download`).

        Downloaders can override this to add some properties to the file (for
        instance, most of them will add "root").
        """
        # Add properties to the file if needed (for safety)
        if 'save_as' not in rfile or rfile['save_as'] is None:
            rfile['save_as'] = rfile['name']
        if self.param:
            if 'param' not in rfile or not rfile['param']:
                rfile['param'] = self.param
        self.files_to_download.append(rfile)

    def set_files_to_download(self, files):
        """
        Convenience method to set the list of files to download.
        """
        self.files_to_download = []
        for file_to_download in files:
            self._append_file_to_download(file_to_download)

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
                        if prefix != '':
                            rfile['name'] = prefix + '/' + rfile['name']
                        self._append_file_to_download(rfile)
                        self.logger.debug('Download:File:MatchRegExp:' + rfile['name'])
                    return
                for direlt in dir_list:
                    subdir = direlt['name']
                    self.logger.debug('Download:File:Subdir:Check:' + subdir)
                    if pattern == '**/*':
                        (subfile_list, subdirs_list) = self.list(prefix + '/' + subdir + '/')
                        self.match([pattern], subfile_list, subdirs_list, prefix + '/' + subdir, True)
                        for rfile in file_list:
                            if pattern == '**/*' or re.match(pattern, rfile['name']):
                                if prefix != '':
                                    rfile['name'] = prefix + '/' + rfile['name']
                                self._append_file_to_download(rfile)
                                self.logger.debug('Download:File:MatchRegExp:' + rfile['name'])
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
                        if prefix != '':
                            rfile['name'] = prefix + '/' + rfile['name']
                        self._append_file_to_download(rfile)
                        self.logger.debug('Download:File:MatchRegExp:' + rfile['name'])
        if not submatch and len(self.files_to_download) == 0:
            raise Exception('no file found matching expressions')

    def set_permissions(self, file_path, file_info):
        '''
        Sets file attributes to remote ones
        '''
        if file_info['year'] and file_info['month'] and file_info['day']:
            ftime = datetime.date(file_info['year'], file_info['month'], file_info['day'])
            settime = time.mktime(ftime.timetuple())
            os.utime(file_path, (settime, settime))

    def download_or_copy(self, available_files, root_dir, check_exists=True):
        '''
        If a file to download is available in available_files, copy it instead of downloading it.

        Update the instance variables files_to_download and files_to_copy

        :param available_files: list of files available in root_dir
        :type available files: list
        :param root_dir: directory where files are available
        :type root_dir: str
        :param check_exists: checks if file exists locally
        :type check_exists: bool
        '''

        self.files_to_copy = []
        # In such case, it forces the download again
        if not available_files:
            return
        available_files.sort(key=lambda x: x['name'])
        self.files_to_download.sort(key=lambda x: x['name'])

        new_files_to_download = []

        test1_tuples = set((d['name'], d['year'], d['month'], d['day'], d['size']) for d in self.files_to_download)
        test2_tuples = set((d['name'], d['year'], d['month'], d['day'], d['size']) for d in available_files)
        new_or_modified_files = [t for t in test1_tuples if t not in test2_tuples]
        new_or_modified_files.sort(key=lambda x: x[0])
        index = 0

        if len(new_or_modified_files) > 0:
            self.logger.debug('Number of remote files: %d' % (len(self.files_to_download)))
            self.logger.debug('Number of local files: %d' % (len(available_files)))
            self.logger.debug('Number of files new or modified: %d' % (len(new_or_modified_files)))
            for dfile in self.files_to_download:
                if index < len(new_or_modified_files) and \
                        dfile['name'] == new_or_modified_files[index][0]:

                    new_files_to_download.append(dfile)
                    index += 1
                else:
                    fileName = dfile["name"]
                    if dfile["name"].startswith('/'):
                        fileName = dfile["name"][1:]
                    if not check_exists or os.path.exists(os.path.join(root_dir, fileName)):
                        dfile['root'] = root_dir
                        self.logger.debug('Copy file instead of downloading it: %s' % (os.path.join(root_dir, dfile['name'])))
                        self.files_to_copy.append(dfile)
                    else:
                        new_files_to_download.append(dfile)
        else:
            # Copy everything
            for dfile in self.files_to_download:
                fileName = dfile["name"]
                if dfile["name"].startswith('/'):
                    fileName = dfile["name"][1:]
                if not check_exists or os.path.exists(os.path.join(root_dir, fileName)):
                    dfile['root'] = root_dir
                    self.files_to_copy.append(dfile)
                else:
                    new_files_to_download.append(dfile)

        self.set_files_to_download(new_files_to_download)

    def _download(self, file_path, rfile):
        '''
        Download one file and return False in case of success and True
        otherwise. This must be implemented in subclasses.
        '''
        raise NotImplementedError()

    def download(self, local_dir, keep_dirs=True):
        '''
        Download remote files to local_dir

        :param local_dir: Directory where files should be downloaded
        :type local_dir: str
        :param keep_dirs: keep file name directory structure or copy file in local_dir directly
        :param keep_dirs: bool
        :return: list of downloaded files
        '''
        self.logger.debug(self.__class__.__name__ + ':Download')
        nb_files = len(self.files_to_download)
        cur_files = 1
        self.offline_dir = local_dir
        for rfile in self.files_to_download:
            if self.kill_received:
                raise Exception('Kill request received, exiting')
            # Determine where to store file (directory and name)
            file_dir = local_dir
            if keep_dirs:
                file_dir = local_dir + '/' + os.path.dirname(rfile['save_as'])
            if file_dir[-1] == "/":
                file_path = file_dir + os.path.basename(rfile['save_as'])
            else:
                file_path = file_dir + '/' + os.path.basename(rfile['save_as'])

            # For unit tests only, workflow will take in charge directory
            # creation before to avoid thread multi access
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            msg = self.__class__.__name__ + ':Download:Progress:'
            msg += str(cur_files) + '/' + str(nb_files)
            msg += ' downloading file ' + rfile['name'] + ' save as ' + rfile['save_as']
            self.logger.debug(msg)
            cur_files += 1
            start_time = datetime.datetime.now()
            start_time = time.mktime(start_time.timetuple())
            error = self._download(file_path, rfile)
            if error:
                rfile['download_time'] = 0
                rfile['error'] = True
                raise Exception(self.__class__.__name__ + ":Download:Error:" + rfile["name"])
            else:
                end_time = datetime.datetime.now()
                end_time = time.mktime(end_time.timetuple())
                rfile['download_time'] = end_time - start_time
            # Set permissions
            self.set_permissions(file_path, rfile)

        return self.files_to_download

    def list(self):
        '''
        List directory

        :return: tuple of file list and dir list
        '''
        pass

    def chroot(self, cwd):
        '''
        Change directory
        '''
        pass

    def close(self):
        '''
        Close connection
        '''
        pass
