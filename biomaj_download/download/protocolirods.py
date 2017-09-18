import logging
import re
import os
import subprocess
from datetime import datetime
import time

from mock.mock import PropertyMock
from biomaj_download.download.interface import DownloadInterface
#import irods
from irods.session import iRODSSession
from irods.models import Collection, DataObject, DataAccess, User



class IRODSDownload(DownloadInterface):
    # To connect to irods session : sess = iRODSSession(host='localhost', port=1247, user='rods', password='rods', zone='tempZone')
    # password : self.credentials
    def __init__(self, protocol, server, remote_dir):
        DownloadInterface.__init__(self)
        logging.debug('Download')
        logging.debug("IRODS_download: "+str(self.param))
        #self.protocol = protocol
        #self.port = server.split(":")[1]
        #self.server = server.split(":")[0]  # name of the remote server : host:port
        #self.remote_dir = remote_dir  # directory on the remote server : zone
        #self.user = self.credentials.split(":")[0]
        #self.password =  self.credentials.split(":")[1]
        #self.session = iRODSSession(host = self.server, port = self.port, user = self.user, password = self.password, zone = self.remote_dir)

    def list(self, directory=''):
        rfiles = []
        rdirs = []
        rfile = {}
        for result in self.session.query(Collection.name, DataObject.name, DataObject.size, DataObject.owner_name, DataObject.modify_time).filter(User.name == self.user).get_results():
            #if the user is biomaj : he will have access to all the irods data (biomaj ressource) : drwxr-xr-x
            #Avoid duplication
            if rfile != {} and rfile['name'] == str(result[DataObject.name]) and date == str(result[DataObject.modify_time]).split(" ")[0].split('-'):
                continue
            rfile = {}
            date = str(result[DataObject.modify_time]).split(" ")[0].split('-')
            rfile['permissions'] = "-rwxr-xr-x"
            rfile['size'] = int(result[DataObject.size])
            rfile['month'] = int(date[1])
            rfile['day'] = int(date[2])
            rfile['year'] = int(date[0])
            rfile['name'] = str(result[DataObject.name])
            rfile['download_path'] = str(result[Collection.name])
            rfiles.append(rfile)
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
        logging.debug('IRODS:Download')
        #try:
        #    os.chdir(local_dir)
        #except TypeError:
        #    logging.error("IRODS:list:Could not find offline_dir")
        nb_files = len(self.files_to_download)
        cur_files = 1
        # give a working directory to copy the file from irods
        remote_dir = self.remote_dir
        for rfile in self.files_to_download:
            if self.kill_received:
                raise Exception('Kill request received, exiting')
            file_dir = local_dir
            if 'save_as' not in rfile or rfile['save_as'] is None:
                rfile['save_as'] = rfile['name']
            if keep_dirs:
                file_dir = local_dir + os.path.dirname(rfile['save_as'])
            file_path = file_dir + '/' + os.path.basename(rfile['save_as'])
            # For unit tests only, workflow will take in charge directory creation before to avoid thread multi access
            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            logging.debug('IRODS:Download:Progress:' + str(cur_files) + '/' + str(nb_files) + ' downloading file ' + rfile['name'])
            logging.debug('IRODS:Download:Progress:' + str(cur_files) + '/' + str(nb_files) + ' save as ' + rfile['save_as'])
            cur_files += 1
            start_time = datetime.now()
            start_time = time.mktime(start_time.timetuple())
            self.remote_dir=rfile['download_path']
            error = self.irods_download(file_path, rfile['name'])
            if error:
                rfile['download_time'] = 0
                rfile['error'] = True
                raise Exception("IRODS:Download:Error:" + rfile['root'] + '/' + rfile['name'])
            end_time = datetime.now()
            end_time = time.mktime(end_time.timetuple())
            rfile['download_time'] = end_time - start_time
            self.set_permissions(file_path, rfile)
        self.remote_dir = remote_dir
        return(self.files_to_download)

    def irods_download(self, file_path, file_to_download):
        error = False
        err_code = 0
        logging.debug('IRODS:IRODS DOwNLOAD')
        path = os.path.dirname(file_path)
        name_file = os.path.basename(file_path)
        try:
            cmd =str(self.protocol) + " " +str(file_to_download)
            logging.error("cmd : "+str(cmd))
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()
            err_code = p.returncode
        except ExceptionRsync as e:
            logging.error("IRODSError:" + str(e))
        if str(err_code) == '3':
            logging.error('Error while downloading: the file already exists. Error with: ' + file_to_download + ' - ' + str(err_code))
            error = True
        elif str(err_code) == '7':
            logging.error('Error while downloading: connection problem. Error with: ' + file_to_download + ' - ' + str(err_code))
            error = True
        elif str(err_code) != '0':
            logging.error('Error while downloading. Error with: ' + file_to_download + ' - ' + str(err_code))
            error = True
        return(error)
        

class ExceptionRsync(Exception):
    def __init__(self, exception_reason):
        self.exception_reason = exception_reason

    def __str__(self):
        return self.exception_reason        
        
        
