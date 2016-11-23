# from future import standard_library
# standard_library.install_aliases()
# from builtins import str
import logging
import re
import os
import subprocess
from datetime import datetime
import time

from biomaj_download.download.interface import DownloadInterface


class RSYNCDownload(DownloadInterface):
    '''
    Base class to download files from rsyncc
    protocol = rsync
    server =
    remote.dir =

    remote.files =
    '''

    def __init__(self, protocol, server, remote_dir):
        DownloadInterface.__init__(self)
        logging.debug('Download')
        self.rootdir = remote_dir
        self.protocol = protocol
        if server and remote_dir:
            self.server = server  # name of the remote server
            self.remote_dir = remote_dir  # directory on the remote server
        else:
            if server:
                self.server = server
                self.remote_dir = ""

    def list(self, directory=''):
        '''
        List server directory

        :return: dict of file and dirs in current directory with details
        '''
        err_code = None
        rfiles = []
        rdirs = []
        logging.debug('RSYNC:List')
        # give a working directory to run rsync
        try:
            os.chdir(self.offline_dir)
        except TypeError:
            logging.error("RSYNC:list:Could not find offline_dir")
        if self.remote_dir and self.credentials:
            cmd = str(self.protocol) + " --list-only " + str(self.credentials) + "@" + str(self.server) + ":" + str(self.remote_dir) + str(directory)
        elif (self.remote_dir and not self.credentials):
            cmd = str(self.protocol) + " --list-only " + str(self.server) + ":" + str(self.remote_dir) + str(directory)
        else:  # Local rsync for unitest
            cmd = str(self.protocol) + " --list-only " + str(self.server) + str(directory)
        try:
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            list_rsync, err = p.communicate()
            self.test_stderr_rsync_message(err)
            self.test_stderr_rsync_error(err)
            err_code = p.returncode
        except ExceptionRsync as e:
            logging.error("RsyncError:" + str(e))
        if err_code != 0:
            logging.error('Error while listing ' + str(err_code))
            return(rfiles, rdirs)
        list_rsync = str(list_rsync.decode('utf-8'))
        lines = list_rsync.rstrip().split("\n")
        for line in lines:
            rfile = {}
            # rsync LIST output is separated by \n
            parts = line.split()
            if not parts:
                continue
            date = parts[2].split('/')
            rfile['permissions'] = parts[0]
            rfile['size'] = int(parts[1].replace(',', ''))
            rfile['month'] = int(date[1])
            rfile['day'] = int(date[2])
            rfile['year'] = int(date[0])
            rfile['name'] = parts[4]
            is_dir = False
            if re.match('^d', rfile['permissions']):
                is_dir = True

            if not is_dir:
                rfiles.append(rfile)
            else:
                rdirs.append(rfile)

        return (rfiles, rdirs)

    def download(self, local_dir, keep_dirs=True):
        '''
        Download remote files to local_dir

        :param local_dir: Directory where files should be downloaded
        :type local_dir: str
        :param keep_dirs: keep file name directory structure or copy file in local_dir directly
        :param keep_dirs: bool
        :return: list of downloaded files
        '''

        logging.debug('RSYNC:Download')
        nb_files = len(self.files_to_download)
        cur_files = 1
        # give a working directory to run rsync
        try:
            os.chdir(self.offline_dir)
        except TypeError:
            logging.error("RSYNC:list:Could not find offline_dir")
        for rfile in self.files_to_download:
            if self.kill_received:
                raise Exception('Kill request received, exiting')
            file_dir = local_dir
            if 'save_as' not in rfile or rfile['save_as'] is None:
                rfile['save_as'] = rfile['name']
            if keep_dirs:
                file_dir = local_dir + '/' + os.path.dirname(rfile['save_as'])
            if re.match('\S*\/$', file_dir):
                file_path = file_dir + '/' + os.path.basename(rfile['save_as'])
            else:
                file_path = file_dir + os.path.basename(rfile['save_as'])
            # For unit tests only, workflow will take in charge directory creation before to avoid thread multi access
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            logging.debug('RSYNC:Download:Progress:' + str(cur_files) + '/' + str(nb_files) + ' downloading file ' + rfile['name'])
            logging.debug('RSYNC:Download:Progress:' + str(cur_files) + '/' + str(nb_files) + ' save as ' + rfile['save_as'])
            cur_files += 1
            start_time = datetime.now()
            start_time = time.mktime(start_time.timetuple())
            error = self.rsync_download(file_path, rfile['name'])
            if error:
                rfile['download_time'] = 0
                rfile['error'] = True
                raise Exception("RSYNC:Download:Error:" + rfile['root'] + '/' + rfile['name'])
            end_time = datetime.now()
            end_time = time.mktime(end_time.timetuple())
            rfile['download_time'] = end_time - start_time
            self.set_permissions(file_path, rfile)
        return(self.files_to_download)

    def rsync_download(self, file_path, file_to_download):
        error = False
        err_code = ''
        logging.debug('RSYNC:RSYNC DOwNLOAD')
        # give a working directory to run rsync
        try:
            os.chdir(self.offline_dir)
        except TypeError:
            logging.error("RSYNC:list:Could not find offline_dir")
        try:
            if self.remote_dir and self.credentials:  # download on server
                cmd = str(self.protocol) + " " + str(self.credentials) + "@" + str(self.server) + ":" + str(self.remote_dir) + str(file_to_download) + " " + str(file_path)
            elif self.remote_dir and not self.credentials:
                cmd = str(self.protocol) + " " + str(self.server) + ":" + str(self.remote_dir) + str(file_to_download) + " " + str(file_path)
            else:  # Local rsync for unitest
                cmd = str(self.protocol) + " " + str(self.server) + str(file_to_download) + " " + str(file_path)
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()
            err_code = p.returncode
            self.test_stderr_rsync_message(stderr)
            self.test_stderr_rsync_error(stderr)
        except ExceptionRsync as e:
            logging.error("RsyncError:" + str(e))
        if err_code != 0:
            logging.error('Error while downloading ' + file_to_download + ' - ' + str(err_code))
            error = True
        return(error)

    def test_stderr_rsync_error(self, stderr):
        stderr = str(stderr.decode('utf-8'))
        if "rsync error" in str(stderr):
            reason = stderr.split(str(self.protocol) + " error:")[1].split("\n")[0]
            raise ExceptionRsync(reason)

    def test_stderr_rsync_message(self, stderr):
        stderr = str(stderr.decode('utf-8'))
        if "rsync:" in str(stderr):
            reason = stderr.split(str(self.protocol) + ":")[1].split("\n")[0]
            raise ExceptionRsync(reason)


class ExceptionRsync(Exception):
    def __init__(self, exception_reason):
        self.exception_reason = exception_reason

    def __str__(self):
        return self.exception_reason
