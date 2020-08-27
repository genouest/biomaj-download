from biomaj_download.download.interface import DownloadInterface
from irods.session import iRODSSession
from irods.exception import iRODSException
from irods.models import DataObject, User


class IRODSDownload(DownloadInterface):

    # This is used only for messages
    real_protocol = "irods"

    def __init__(self, server, remote_dir):
        DownloadInterface.__init__(self)
        self.port = 1247
        self.remote_dir = remote_dir  # directory on the remote server including zone
        self.rootdir = remote_dir
        self.user = None
        self.password = None
        self.server = server
        self.zone = remote_dir.split("/")[0]

    def _append_file_to_download(self, rfile):
        if 'root' not in rfile or not rfile['root']:
            rfile['root'] = self.rootdir
        super(IRODSDownload, self)._append_file_to_download(rfile)

    def set_param(self, param):
        # param is a dictionary which has the following form :
        # {'password': u'biomaj', 'user': u'biomaj', 'port': u'port'}
        # port is optional
        self.param = param
        self.user = str(param['user'])
        self.password = str(param['password'])
        if 'port' in param:
            self.port = int(param['port'])

    def list(self, directory=''):
        self._network_configuration()
        rfiles = []
        rdirs = []
        rfile = {}
        date = None
        # Note that iRODS raise errors when trying to use the results
        # and not after query(). Therefore, the whole loop is inside
        # try/catch.
        try:
            query = self.session.query(DataObject.name, DataObject.size,
                                       DataObject.owner_name, DataObject.modify_time)
            results = query.filter(User.name == self.user).get_results()
            for result in results:
                # Avoid duplication
                if rfile != {} and rfile['name'] == str(result[DataObject.name]) \
                   and date == str(result[DataObject.modify_time]).split(" ")[0].split('-'):
                    continue
                rfile = {}
                date = str(result[DataObject.modify_time]).split(" ")[0].split('-')
                rfile['permissions'] = "-rwxr-xr-x"
                rfile['size'] = int(result[DataObject.size])
                rfile['month'] = int(date[1])
                rfile['day'] = int(date[2])
                rfile['year'] = int(date[0])
                rfile['name'] = str(result[DataObject.name])
                rfiles.append(rfile)
        except Exception as e:
            msg = 'Error while listing ' + self.remote_dir + ' - ' + repr(e)
            self.logger.error(msg)
            raise e
        finally:
            self.session.cleanup()
        return (rfiles, rdirs)

    def _network_configuration(self):
        self.session = iRODSSession(host=self.server, port=self.port,
                                    user=self.user, password=self.password,
                                    zone=self.zone)

    def _download(self, file_path, rfile):
        error = False
        self.logger.debug('IRODS:IRODS DOWNLOAD')
        try:
            # iRODS don't like multiple "/"
            if rfile['root'][-1] == "/":
                file_to_get = rfile['root'] + rfile['name']
            else:
                file_to_get = rfile['root'] + "/" + rfile['name']
            # Write the file to download in the wanted file_dir with the
            # python-irods iget
            self.session.data_objects.get(file_to_get, file_path)
        except iRODSException as e:
            error = True
            self.logger.error(self.__class__.__name__ + ":Download:Error:Can't get irods object " + file_to_get)
            self.logger.error(self.__class__.__name__ + ":Download:Error:" + repr(e))

        if error:
            return error

        # Our part is done so call parent _download
        return super(IRODSDownload, self)._download(file_path, rfile)
