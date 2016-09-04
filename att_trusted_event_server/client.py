__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

from att_event_engine.att import HttpClient
import broker

class Client(HttpClient):
    """
    reimplementation of the client which uses a shared broker connection.
    """

    def __init__(self):
        """
        create the client object
        :param broker: the amqp broker connection to user
        """
        super(Client, self).__init__()
        self.divider = '.'
        self.wildcard = '*'
        self.multi_wildcard = '#'