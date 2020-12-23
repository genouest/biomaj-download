import os
import logging
import datetime
import time
import re
import copy

import tenacity
from simpleeval import simple_eval, ast

from biomaj_core.utils import Utils
from biomaj_core.config import BiomajConfig


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

    #
    # Constants to parse retryer
    #
    # Note that due to the current implementation of operators, tenacity allows
    # nonsensical operations. For example the following snippets are valid:
    # stop_after_attempt(1, 2) + 4
    # stop_after_attempt(1, 2) + stop_none.
    # Of course, trying to use those wait policies will raise cryptic errors.
    # The situation is similar for stop conditions.
    # See https://github.com/jd/tenacity/issues/211.
    # To avoid such errors, we test the objects in _set_retryer.
    #
    # Another confusing issue is that stop_never is an object (instance of the
    # class _stop_never). For parsing, if we consider stop_never as a
    # function then both "stop_never" and "stop_never()" are parsed correctly
    # but the later raises error. Considering it has a name is slightly more
    # clear (since then we must write "stop_none" as we do when we use tenacity
    # directly). For consistency, we create a name for wait_none (as an
    # instance of the class wait_none).
    #

    # Functions available when parsing stop condition: those are constructors
    # of stop conditions classes (then using them will create objects). Note
    # that there is an exception for stop_never.
    ALL_STOP_CONDITIONS = {
        # "stop_never": tenacity.stop._stop_never,  # In case, we want to use it like a function (see above)
        "stop_when_event_set": tenacity.stop_when_event_set,
        "stop_after_attempt": tenacity.stop_after_attempt,
        "stop_after_delay": tenacity.stop_after_delay,
        "stop_any": tenacity.stop_any,  # Similar to |
        "stop_all": tenacity.stop_all,  # Similar to &
    }

    # tenacity.stop_never is an instance of _stop_never, not a class so we
    # import it as a name.
    ALL_STOP_NAMES = {
        "stop_never": tenacity.stop_never,
    }

    # Operators for stop conditions: | means to stop if one of the conditions
    # is True, & means to stop if all the conditions are True.
    ALL_STOP_OPERATORS = {
        ast.BitOr: tenacity.stop.stop_base.__or__,
        ast.BitAnd: tenacity.stop.stop_base.__and__,
    }

    # Functions available when parsing wait policy: those are constructors
    # of wait policies classes (then using them will create objects). Note
    # that there is an exception for wait_none.
    ALL_WAIT_POLICIES = {
        # "wait_none": tenacity.wait_none,  # In case, we want to use it like a function (see above)
        "wait_fixed": tenacity.wait_fixed,
        "wait_random": tenacity.wait_random,
        "wait_incrementing": tenacity.wait_incrementing,
        "wait_exponential": tenacity.wait_exponential,
        "wait_random_exponential": tenacity.wait_random_exponential,
        "wait_combine": tenacity.wait_combine,  # Sum of wait policies (similar to +)
        "wait_chain": tenacity.wait_chain,  # Give a list of wait policies (one for each attempt)
    }

    # Create an instance of wait_none to use it like a constant.
    ALL_WAIT_NAMES = {
        "wait_none": tenacity.wait.wait_none()
    }

    # Operators for wait policies: + means to sum waiting times of wait
    # policies.
    ALL_WAIT_OPERATORS = {
        ast.Add: tenacity.wait.wait_base.__add__
    }

    @staticmethod
    def is_true(download_error):
        """Method used by retryer to determine if we should retry to downlaod a
        file based on the return value of method:`_download` (passed as the
        argument): we must retry while this value is True.

        See method:`_set_retryer`.
        """
        return download_error is True

    @staticmethod
    def return_last_value(retry_state):
        """Method used by the retryer to determine the return value of the
        retryer: we return the result of the last attempt.

        See method:`_set_retryer`.
        """
        return retry_state.outcome.result()

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
        # TODO: Don't store default values in BiomajConfig.DEFAULTS for
        # wait_policy and stop_condition
        # Construct default retryer (may be replaced in set_options)
        self._set_retryer(
            BiomajConfig.DEFAULTS["stop_condition"],
            BiomajConfig.DEFAULTS["wait_policy"]
        )

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
        # If stop_condition or wait_policy is specified, we reconstruct the retryer
        if "stop_condition" or "wait_policy" in options:
            stop_condition = options.get("stop_condition", BiomajConfig.DEFAULTS["stop_condition"])
            wait_policy = options.get("wait_policy", BiomajConfig.DEFAULTS["wait_policy"])
            self._set_retryer(stop_condition, wait_policy)

    def _set_retryer(self, stop_condition, wait_policy):
        """
        Add a retryer to retry the current download if it fails.
        """
        # Try to construct stop condition
        if isinstance(stop_condition, tenacity.stop.stop_base):
            # Use the value directly
            stop_cond = stop_condition
        elif isinstance(stop_condition, str):
            # Try to parse the string
            try:
                stop_cond = simple_eval(stop_condition,
                                        functions=self.ALL_STOP_CONDITIONS,
                                        operators=self.ALL_STOP_OPERATORS,
                                        names=self.ALL_STOP_NAMES)
                # Check that it is an instance of stop_base
                if not isinstance(stop_cond, tenacity.stop.stop_base):
                    raise ValueError(stop_condition + " doesn't yield a stop condition")
                # Test that this is a correct stop condition by calling it.
                # We use a deepcopy to be sure to not alter the object (even
                # if it seems that calling a wait policy doesn't modify it).
                try:
                    s = copy.deepcopy(stop_cond)
                    s(tenacity.compat.make_retry_state(0, 0))
                except Exception:
                    raise ValueError(stop_condition + " doesn't yield a stop condition")
            except Exception as e:
                raise ValueError("Error while parsing stop condition: %s" % e)
        else:
            raise TypeError("Expected tenacity.stop.stop_base or string, got %s" % type(stop_condition))
        # Try to construct wait policy
        if isinstance(wait_policy, tenacity.wait.wait_base):
            # Use the value directly
            wait_pol = wait_policy
        elif isinstance(wait_policy, str):
            # Try to parse the string
            try:
                wait_pol = simple_eval(wait_policy,
                                       functions=self.ALL_WAIT_POLICIES,
                                       operators=self.ALL_WAIT_OPERATORS,
                                       names=self.ALL_WAIT_NAMES)
                # Check that it is an instance of wait_base
                if not isinstance(wait_pol, tenacity.wait.wait_base):
                    raise ValueError(wait_policy + " doesn't yield a wait policy")
                # Test that this is a correct wait policy by calling it.
                # We use a deepcopy to be sure to not alter the object (even
                # if it seems that calling a stop condition doesn't modify it).
                try:
                    w = copy.deepcopy(wait_pol)
                    w(tenacity.compat.make_retry_state(0, 0))
                except Exception:
                    raise ValueError(wait_policy + " doesn't yield a wait policy")
            except Exception as e:
                raise ValueError("Error while parsing wait policy: %s" % e)
        else:
            raise TypeError("Expected tenacity.stop.wait_base or string, got %s" % type(wait_policy))

        self.retryer = tenacity.Retrying(
            stop=stop_cond,
            wait=wait_pol,
            retry_error_callback=self.return_last_value,
            retry=tenacity.retry_if_result(self.is_true),
            reraise=True
        )

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
        # Remove duplicate */* if any
        rfile['name'] = re.sub('//+', '/', rfile['name'])
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
        otherwise.

        Subclasses that override this method must call this implementation
        at the end to perform test on archives.

        Note that this method is executed inside a retryer.
        '''
        error = False
        # Check that the archive is correct
        if not self.skip_check_uncompress:
            archive_status = Utils.archive_check(file_path)
            if not archive_status:
                self.logger.error('Archive is invalid or corrupted, deleting file and retrying download')
                error = True
                if os.path.exists(file_path):
                    os.remove(file_path)
        return error

    def _network_configuration(self):
        '''
        Perform some configuration before network operations (list and
        download). This must be implemented in subclasses.
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
        self._network_configuration()
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
            error = self.retryer(self._download, file_path, rfile)
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
