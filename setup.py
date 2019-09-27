try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

from distutils.command.install import install
import os


here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.md')) as f:
    README = f.read()
with open(os.path.join(here, 'CHANGES.txt')) as f:
    CHANGES = f.read()


config = {
    'description': 'BioMAJ download service',
    'long_description': README + '\n\n' + CHANGES,
    'long_description_content_type': 'text/markdown',
    'author': 'Olivier Sallou',
    'url': 'http://biomaj.genouest.org',
    'download_url': 'http://biomaj.genouest.org',
    'author_email': 'olivier.sallou@irisa.fr',
    'version': '3.0.27',
     'classifiers': [
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4'
    ],
    'install_requires': [
                         'biomaj_core',
                         'biomaj_zipkin',
                         'pycurl',
                         'ftputil',
                         'py-bcrypt',
                         'pika==0.13.0',
                         'redis',
                         'PyYAML',
                         'flask',
                         'python-consul',
                         'prometheus_client>=0.0.18',
                         'protobuf',
                         'requests',
                         'humanfriendly',
                         'python-irodsclient'
                        ],
    'tests_require': ['nose', 'mock'],
    'test_suite': 'nose.collector',
    'packages': find_packages(),
    'include_package_data': True,
    'scripts': ['bin/biomaj_download_consumer.py'],
    'name': 'biomaj_download'
}

setup(**config)
