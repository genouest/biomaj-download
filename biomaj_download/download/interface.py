import os
import logging
import datetime
import time
import re


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
    Main interface that all downloaders must extend
    '''

    files_num_threads = 4

    def __init__(self):
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
        self.timeout = 3600 * 24
        # Optional save target for single file downloaders
        self.save_as = None
        self.logger = logging.getLogger('biomaj')
        self.param = None
        self.method = None
        self.protocol = None
        self.server = None
        self.offline_dir = None

    def set_offline_dir(self, offline_dir):
        self.offline_dir = offline_dir

    def set_server(self, server):
        self.server = server

    def set_protocol(self, protocol):
        self.protocol = protocol

    def set_files_to_download(self, files):
        self.files_to_download = files
        for file_to_download in self.files_to_download:
            if self.param:
                if 'param' not in file_to_download or not file_to_download['param']:
                    file_to_download['param'] = self.param

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
                if not dir_list and pattern == '**/*':
                    # Take all and no more dirs, take all files
                    for rfile in file_list:
                        rfile['root'] = self.rootdir
                        if prefix != '':
                            rfile['name'] = prefix + '/' + rfile['name']
                        self.files_to_download.append(rfile)
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
                                rfile['root'] = self.rootdir
                                if prefix != '':
                                    rfile['name'] = prefix + '/' + rfile['name']
                                self.files_to_download.append(rfile)
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
                        rfile['root'] = self.rootdir
                        if prefix != '':
                            rfile['name'] = prefix + '/' + rfile['name']
                        self.files_to_download.append(rfile)
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
                    if not check_exists or os.path.exists(os.path.join(root_dir, dfile['name'])):
                        dfile['root'] = root_dir
                        self.logger.debug('Copy file instead of downloading it: %s' % (os.path.join(root_dir, dfile['name'])))
                        self.files_to_copy.append(dfile)
                    else:
                        new_files_to_download.append(dfile)

        else:
            # Copy everything
            for dfile in self.files_to_download:
                if not check_exists or os.path.exists(os.path.join(root_dir, dfile['name'])):
                    dfile['root'] = root_dir
                    self.files_to_copy.append(dfile)
                else:
                    new_files_to_download.append(dfile)

        self.files_to_download = new_files_to_download

    def download(self, local_dir):
        '''
        Download remote files to local_dir

        :param local_dir: Directory where files should be downloaded
        :type local_dir: str
        :return: list of downloaded files
        '''
        pass

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

    def set_credentials(self, userpwd):
        '''
        Set credentials in format user:pwd

        :param userpwd: credentials
        :type userpwd: str
        '''
        self.credentials = userpwd

    def close(self):
        '''
        Close connection
        '''
        pass
