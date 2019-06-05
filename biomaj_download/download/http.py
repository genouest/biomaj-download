import pycurl
import re
import hashlib
import datetime

import humanfriendly

from biomaj_core.utils import Utils
from biomaj_download.download.ftp import FTPDownload

try:
    from io import BytesIO
except ImportError:
    from StringIO import StringIO as BytesIO


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


class HTTPDownload(FTPDownload):
    '''
    Base class to download files from HTTP

    Makes use of http.parse.dir.line etc.. regexps to extract page information

    protocol=http
    server=ftp.ncbi.nih.gov
    remote.dir=/blast/db/FASTA/

    remote.files=^alu.*\\.gz$

    '''

    def __init__(self, protocol, host, rootdir, http_parse=None):
        FTPDownload.__init__(self, protocol, host, rootdir)
        self.http_parse = http_parse

    def _parse_result(self, result):
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
                        date_object = datetime.datetime.strptime(date, self.http_parse.file_date_format.replace('%%', '%'))
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
                    today = datetime.datetime.now()
                    date = '%s-%s-%s' % (today.year, today.month, today.day)
                    rfile['month'] = today.month
                    rfile['day'] = today.day
                    rfile['year'] = today.year
                rfile['name'] = foundfile[self.http_parse.file_name - 1]
                filehash = (rfile['name'] + str(date) + str(rfile['size'])).encode('utf-8')
                rfile['hash'] = hashlib.md5(filehash).hexdigest()
                rfiles.append(rfile)

        return (rfiles, rdirs)
