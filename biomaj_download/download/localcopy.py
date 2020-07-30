import os
import datetime
import hashlib

from biomaj_core.utils import Utils
from biomaj_download.download.interface import DownloadInterface


class LocalDownload(DownloadInterface):
    '''
    Base class to copy file from local system.

    protocol=cp
    server=localhost
    remote.dir=/blast/db/FASTA/

    remote.files=^alu.*\\.gz$

    Note that we redefine download and list in such a way that we don't need to
    define _download and _network_configuration.
    '''

    def __init__(self, rootdir, use_hardlinks=False):
        DownloadInterface.__init__(self)
        self.logger.debug('Download')
        self.rootdir = rootdir
        self.use_hardlinks = use_hardlinks

    def _append_file_to_download(self, rfile):
        if 'root' not in rfile or not rfile['root']:
            rfile['root'] = self.rootdir
        super(LocalDownload, self)._append_file_to_download(rfile)

    def download(self, local_dir):
        '''
        Copy local files to local_dir

        :param local_dir: Directory where files should be copied
        :type local_dir: str
        :return: list of downloaded files
        '''
        self.logger.debug('Local:Download')
        Utils.copy_files(self.files_to_download, local_dir,
                         use_hardlinks=self.use_hardlinks,
                         lock=self.mkdir_lock)
        for rfile in self.files_to_download:
            rfile['download_time'] = 0

        return self.files_to_download

    def list(self, directory=''):
        '''
        List FTP directory

        :return: tuple of file and dirs in current directory with details
        '''
        self.logger.debug('Download:List:' + self.rootdir + directory)
        # lets walk through each line

        rfiles = []
        rdirs = []

        try:
            files = [f for f in os.listdir(self.rootdir + directory)]
        except Exception as e:
            msg = 'Error while listing ' + self.rootdir + ' - ' + str(e)
            self.logger.error(msg)
            raise e
        for file_in_files in files:
            rfile = {}
            fstat = os.stat(os.path.join(self.rootdir + directory, file_in_files))

            rfile['permissions'] = str(fstat.st_mode)
            rfile['group'] = str(fstat.st_gid)
            rfile['user'] = str(fstat.st_uid)
            rfile['size'] = fstat.st_size
            fstat_mtime = datetime.datetime.fromtimestamp(fstat.st_mtime)
            rfile['month'] = fstat_mtime.month
            rfile['day'] = fstat_mtime.day
            rfile['year'] = fstat_mtime.year
            rfile['name'] = file_in_files
            filehash = (rfile['name'] + str(fstat.st_mtime) + str(rfile['size'])).encode('utf-8')
            rfile['hash'] = hashlib.md5(filehash).hexdigest()

            is_dir = False
            if os.path.isdir(os.path.join(self.rootdir + directory, file_in_files)):
                is_dir = True

            if not is_dir:
                rfiles.append(rfile)
            else:
                rdirs.append(rfile)
        return (rfiles, rdirs)

    def chroot(self, cwd):
        self.logger.debug('Download: change dir ' + cwd)
        os.chdir(cwd)
