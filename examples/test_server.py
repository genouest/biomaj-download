from biomaj_download.downloadservice import DownloadService


dserv = DownloadService('config.yml')
dserv.wait_for_messages()
