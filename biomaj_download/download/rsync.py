# from future import standard_library
# standard_library.install_aliases()
# from builtins import str
import re
import subprocess

from biomaj_download.download.interface import DownloadInterface


class RSYNCDownload(DownloadInterface):
    '''
    Base class to download files from rsync
    protocol = rsync
    server =
    remote.dir =

    remote.files =
    '''

    # This is used to forge the command
    real_protocol = "rsync"

    def __init__(self, server, rootdir):
        DownloadInterface.__init__(self)
        self.logger.debug('Download')
        # If rootdir is not given, we are in local mode. In this case, server
        # is interpreted as rootdir
        self.local_mode = not rootdir
        if not self.local_mode:
            self.server = server  # name of the remote server
            self.rootdir = rootdir  # directory on the remote server
        else:
            self.server = None
            self.rootdir = server

    def _append_file_to_download(self, rfile):
        if 'root' not in rfile or not rfile['root']:
            rfile['root'] = self.rootdir
        super(RSYNCDownload, self)._append_file_to_download(rfile)

    def _remote_file_name(self, rfile):
        # rfile['root'] is set to self.rootdir. We don't use os.path.join
        # because rfile['name'] may starts with /
        url = rfile['root'] + "/" + rfile['name']
        if not self.local_mode:
            url = self.server + ":" + url
        return re.sub("/{2,}", "/", url)

    def _network_configuration(self):
        '''
        Perform some configuration before network operations (list and
        download).
        '''
        pass

    def _download(self, file_path, rfile):
        error = False
        err_code = ''
        url = self._remote_file_name(rfile)
        # Create the rsync command
        if self.credentials:
            cmd = str(self.real_protocol) + " " + str(self.credentials) + "@" + url + " " + str(file_path)
        else:
            cmd = str(self.real_protocol) + " " + url + " " + str(file_path)
        self.logger.debug('RSYNC:RSYNC DOWNLOAD:' + cmd)
        # Launch the command (we are in offline_dir)
        try:
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()
            err_code = p.returncode
            self.test_stderr_rsync_message(stderr)
            self.test_stderr_rsync_error(stderr)
        except ExceptionRsync as e:
            self.logger.error(str(self.real_protocol) + " error:" + str(e))
        if err_code != 0:
            self.logger.error('Error while downloading ' + rfile["name"] + ' - ' + str(err_code))
            error = True
        if error:
            return error

        # Our part is done so call parent _download
        return super(RSYNCDownload, self)._download(file_path, rfile)

    def test_stderr_rsync_error(self, stderr):
        stderr = str(stderr.decode('utf-8'))
        if "rsync error" in str(stderr):
            reason = stderr.split(str(self.real_protocol) + " error:")[1].split("\n")[0]
            raise ExceptionRsync(reason)

    def test_stderr_rsync_message(self, stderr):
        stderr = str(stderr.decode('utf-8'))
        if "rsync:" in str(stderr):
            reason = stderr.split(str(self.real_protocol) + ":")[1].split("\n")[0]
            raise ExceptionRsync(reason)

    def list(self, directory=''):
        '''
        List server directory

        :return: dict of file and dirs in current directory with details
        '''
        err_code = None
        rfiles = []
        rdirs = []
        self.logger.debug('RSYNC:List')
        if self.local_mode:
            remote = str(self.rootdir) + str(directory)
        else:
            remote = str(self.server) + ":" + str(self.rootdir) + str(directory)
        if self.credentials:
            remote = str(self.credentials) + "@" + remote
        cmd = str(self.real_protocol) + " --list-only --no-motd " + remote
        try:
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            list_rsync, err = p.communicate()
            self.test_stderr_rsync_message(err)
            self.test_stderr_rsync_error(err)
            err_code = p.returncode
            if err_code != 0:
                msg = 'Error while listing ' + remote + ' - ' + str(err_code)
                self.logger.error(msg)
                raise Exception(msg)
        except ExceptionRsync as e:
            msg = 'Error while listing ' + remote + ' - ' + str(e)
            self.logger.error(msg)
            raise e
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


class ExceptionRsync(Exception):
    def __init__(self, exception_reason):
        self.exception_reason = exception_reason

    def __str__(self):
        return self.exception_reason
