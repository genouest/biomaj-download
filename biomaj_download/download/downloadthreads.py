from builtins import str
from builtins import range
#import os
import logging
#import datetime
#import time
#import re
import threading
import copy
#import tarfile
#import zipfile
import traceback

class DownloadThread(threading.Thread):

    def __init__(self, ds, queue):
        '''
        Download thread to download a list of files

        :param downloader: downloader to use
        :type downloader: :class:`biomaj.download.interface.DownloadInterface`
        :param local_dir: directory to download files
        :type local_dir: str
        '''
        threading.Thread.__init__(self)
        self.queue = queue
        self._stopevent = threading.Event()
        self.error = 0
        self.files_to_download = 0
        self.ds = ds

    def run(self):
        logging.info('Start download thread')
        try:
            message = self.queue.get(False)
        except Exception:
            return
        while message:
            files = self.ds.local_download(message)
            if files is None:
                self.error += 1
            self.files_to_download += 1
            self.queue.task_done()
            try:
                message = self.queue.get(False)
            except Exception:
                break

    def stop(self):
        self._stopevent.set()


DownloadThread.MKDIR_LOCK = threading.Lock()
