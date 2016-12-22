__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

import sys
#setup the correct when_platform module (platform specific)
import att_event_engine.when_platform
del sys.modules['att_event_engine.when_platform']
sys.modules['att_event_engine.when_platform'] = __import__('att_trusted_event_server.when_server').when_server
import att_event_engine.when_platform

import broker
import callbackObject
import client
import att_event_engine.resources as resources

class IotApplication:
    """provides the main entry point for an Iot application"""
    def __init__(self, username, pwd, api, brokerName, appName):
        """setup app
        Must be done before any rule is declared.
        :param username:
        :param pwd:
        :param api:
        :param brokerName:
        :param appName: the name of the application. Used to store temp values in redis.
        """

        is_connected = broker.connect(username, pwd, brokerName)
        while not is_connected:                         #no point in trying to go any further if we can't connect here.
            is_connected = broker.reconnect()
        self.api = api
        self.att = client.Client()
        resources.defaultconnection = self.att
        callbackObject.AppName = appName


    def run(self):
        """the main loop.
        Must be called when all the rules are loaded.
        """
        broker.process()

    def stop(self):
        """
        stops the broker.
        :return: None
        """
        broker.stop()
