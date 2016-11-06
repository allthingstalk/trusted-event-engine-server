__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

#connect to the broker and monitor the correct topics so that the required rules can be triggered.

import logging
logger = logging.getLogger('broker')
import pika, ssl
import threading
from time import sleep

channel = None
_subscriptions = []                     # the list of subscriptions that are currently registered. This is kept so that the connection can be recreated if need be.
_subscriptions_lock = threading.Lock()  # subscriptions can be accessed from multiple threads, need to make access secure.
_isRunning = False                       # to stop the thread
_parameters = None                      # connection params


def connect(username, pwd, brokerName):
    """connect to the broker"""
    global channel, _parameters
    try:
        _parameters = pika.URLParameters("amqps://{}:{}@{}/space-maker-vhost?heartbeat_interval=30&socket_timeout=1".format(username, pwd, brokerName))
        connection = pika.BlockingConnection(_parameters)
        channel = connection.channel()
        return True
    except:
        logger.exception("broker connection failure")
        return False

def reconnect():
    """
    tries to recreate a broker connection if it was closed somehow after opening it.
    :return:
    """
    global channel
    try:
        connection = pika.BlockingConnection(_parameters)
        channel = connection.channel()
        _subscriptions_lock.acquire()
        try:
            for topic, callbackObj in _subscriptions:
                _internalsubscribe_to(topic, callbackObj)
        finally:
            _subscriptions_lock.release()
    except:
        logger.exception("broker communication setup failure")

def process():
    """consume messages from the queue and execute the rules
    Note: this starts a new thread, so it is a none blocking call.
    """
    global _isRunning
    _isRunning = True
    mq_recieve_thread = threading.Thread(target=run)
    mq_recieve_thread.daemon = True                   # makes certain that when main thread terminates, this one is also terminated.
    mq_recieve_thread.start()

def stop():
    """
    stops the broker
    :return:
    """
    global _isRunning
    _isRunning = False
    if channel:
        channel.close()

def run():
    """runs the channel consumer. Makes certain that if the connection is lost, it is reconnected"""
    global connection, channel
    while _isRunning:
        try:
            if channel:
                if len(_subscriptions) > 0:  # don't start conuming when there are no subscriptions, this doesn't work
                    channel.start_consuming()
                else:
                    channel.connection.process_data_events()    # make certain that hearbeat is processed
                    sleep(2)                    # no need to loop continuosly if there are no subscriptions, give the cpu some rest until we have something to monitor.
        except:
            logger.exception("broker communication failure")
            channel = None
        if _isRunning and len(_subscriptions) > 0:  # don't try to reconnect if there are no subscriptions, no need for this part of the code.
            reconnect()


def subscribeTo(topic, callbackObj):
    """
    subscribe to the topic so that the callback function in the callback object will be called.
    This is thread safe.
    :param topic:
    :param callback: function to call
    :return:
    """
    _internalsubscribe_to(topic, callbackObj)
    _subscriptions_lock.acquire()
    try:
        _subscriptions.append((topic, callbackObj))
    finally:
        _subscriptions_lock.release()

def _internalsubscribe_to(topic, callbackObj):
    """
    subscribe wthout using the lock or adding the topic/callback to the internal list.
    :param topic:
    :param callbackObj:function to call
    :return:
    """
    if channel.is_open:
        queue = channel.queue_declare(exclusive=True)
        queue_name = queue.method.queue
        channel.queue_bind(exchange='outbound', queue=queue_name, routing_key=topic)
        channel.basic_qos(prefetch_count=1)  # make certain that we only receive 1 message at a time
        channel.basic_consume(callbackObj.callback, queue=queue_name, no_ack=True)
        logger.info("subscribed to {}".format(topic))
    else:
        logger.warn("broker connection closed, can't register topic subscription, retrying when connectio is reopened")


def sendValue(value, topic, exchange='outbound'):
    global channel
    if not channel:
        logger.error("broker connection not initialized, call connect first")
    if channel.is_open == False:
        logger.warn("broker lost connection, reconnecting")
        channel = None
        reconnect()
    sent = False
    retryCount = 0
    while not sent and retryCount < 10:
        try:
            channel.publish(exchange=exchange, routing_key=topic, body=str(value))
            sent = True
        except:
            if retryCount < 10:
                logger.exception("failed to send value, retrying")
            else:
                logger.exception("failed to send value, this was the last attempt.")
            reconnect()
            retryCount += 1