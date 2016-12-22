__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

import logging
import json
import att_event_engine.resources as resources
from att_event_engine.timer import Timer
import redis
import settings


_memStore = redis.StrictRedis(host=settings.redisHost, port=settings.redisPort, db=settings.redisDB, password=settings.redisPwd)

AppName = ""

class CallbackObject(object):
    """contains the references to all the code objects that have subsribed for the same topic"""

    def __init__(self, condition, callback):
        """

        :type callback: function
        :param callback: function that should be called
        """
        self.func = callback
        self.condition = condition

    def callback(self, ch, method, properties, body):
        """
        run the modules. This is the broker callback function.
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        try:
            topicParts = method.routing_key.split('.')
            if topicParts[-1] != 'timer':
                value = json.loads(body)
                asset = value['Id']
                if value and "Value" in value:                                  # bugfix: we sometimes get with capitals, sometimes without. move everything to small capitals.
                    value['value'] = value['Value']
                resources.valueStore[asset] = value
                resources.trigger = resources.Asset(asset)                      # we use the default connection here, which is the
            elif hasattr(self, 'timer'):
                #context = resources.buildFromTopic(topicParts[:-2])             # remove the last 2 items from the topic: timer and timer name
                #timer = Timer(context, topicParts[-1])
                resources.trigger = self.timer
            else:
                resources.trigger = None

            callbackName = "{}_{}".format(AppName, self.callback.__name__)
            if self.condition:
                if self.condition():
                    if _memStore.get(
                            callbackName) != 'True':  # we store the conditional value in memory, not yet in disk
                        _memStore.set(callbackName, True)
                        self.func()
                else:
                    _memStore.set(callbackName, False)
            else:
                self.func()
        except:
            logging.exception("failed to run callback for " + str(properties))