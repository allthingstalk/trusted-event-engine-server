__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

#platform specific (server vs user) code for the 'When' functionality

import logging
logger = logging.getLogger('when')

from att_event_engine.timer import Timer
import att_event_engine.att as att
from callbackObj import CallbackObj
import broker


def _registerAssetToMonitor(asset, callbackObj):
    topics = asset.getTopics()
    for topic in topics:
        monitor = att.SubscriberData(asset.connection)
        monitor.id = topic
        monitor.direction = 'in'
        if isinstance(asset, Timer):
            monitor.level = 'timer'
            callbackObj.timer = asset  # keep a refernce to the timer inside the callback, so we know which one went off.
        topicStr = monitor.getTopic(divider='.', wildcard='*')
        broker.subscribeTo(topicStr, callbackObj)


def registerMonitor(assets, condition, callback):
    """registers the condition and callback for the specified list of asset id's
    :param assets: list of asset objects to monitor with the same condition
    :param condition: function that evaulaties to true or false. When None, 'True' is always presumed (on every change)
    :param callback: the function to call when the condition evaulates to true after an event was raised for the specified asset.
    """
    if hasattr(callback, '_callbackObj'):
        callbackObj = callback._callbackObj
    else:
        callbackObj = CallbackObj(condition, callback)
        callback._callbackObj = callbackObj
    for asset in assets:
        _registerAssetToMonitor(asset, callbackObj)


def appendToMonitorList(callback, toMonitor):
    """
    Adds an element to the list of items that are being monitored for the specified function.
    :param callback: A function that has previously been decorated with a 'When' clause.
    :param toMonitor: a resource to monitor (asset, device, gateway, timer)
    :return: None
    """
    callbackObj = callback._callbackObj
    _registerAssetToMonitor(toMonitor, callbackObj)

def removeFromMonitorList(callback, toRemove):
    """
    removes an element from the list of itmes that are being monitored for the specified function.
    :param callback: function that serves as callback.
    :param toRemove:
    :return:
    """
    callbackObj = callback._callbackObj
    topics = toRemove.getTopics()
    for topic in topics:
        monitor = att.SubscriberData(toRemove.connection)
        monitor.id = topic
        monitor.direction = 'in'
        if isinstance(toRemove, Timer):
            monitor.level = 'timer'
        topicStr = monitor.getTopic(divider='.', wildcard='*')
        raise NotImplemented()