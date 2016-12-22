#from distutils.core import setup
from setuptools import *

setup(
    name='att_trusted_event_server',
    version='0.2.3',
    install_requires=['redis', 'pika'],
    packages=['att_trusted_event_server'],
    url='',
    license='',
    author='Jan Bogaerts',
    author_email='',
    description=''
)
