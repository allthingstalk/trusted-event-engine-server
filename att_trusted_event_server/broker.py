__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

#connect to the broker and monitor the correct topics so that the required rules can be triggered.

import logging
import pika, ssl
import threading
from callbackObj import CallbackObj

channel = None
_subscribtions = []                     # the list of subscriptions that are currently registered. This is kept so that the connection can be recreated if need be.
_subscriptions_lock = threading.Lock()  # subscriptions can be accessed from multiple threads, need to make access secure.
_isRunning = True                       # to stop the thread
_parameters = None                      # connection params


def connect(username, pwd, brokerName):
    """connect to the broker"""
    global channel, _parameters
    _parameters = pika.URLParameters("amqps://{}:{}@{}/space-maker-vhost?heartbeat_interval=30".format(username, pwd, brokerName))
    #connection = pika.BlockingConnection(pika.ConnectionParameters(brokerName, credentials=credentials, virtual_host="space-maker-vhost", ssl=True, ssl_options=ssl_options))
    connection = pika.BlockingConnection(_parameters)
    channel = connection.channel()


def process():
    """consume messages from the queue and execute the rules
    Note: this starts a new thread, so it is a none blocking call.
    """
    print(' [*] Waiting for messages.')
    mq_recieve_thread = threading.Thread(target=channel.run)
    mq_recieve_thread.setDaemon()                   # makes certain that when main thread terminates, this one is also terminated.
    mq_recieve_thread.start()

def stop():
    """
    stops the broker
    :return:
    """
    global _isRunning
    _isRunning = False
    channel.close()

def run():
    """runs the channel consumer. Makes certain that if the connection is lost, it is reconnected"""
    global connection, channel
    while _isRunning:
        channel.start_consuming()
        if _isRunning:
            connection = pika.BlockingConnection(_parameters)
            channel = connection.channel()
            _subscriptions_lock.acquire()
            try:
                for topic, callbackObj in _subscribtions:
                    _internalsubscribe_to(topic, callbackObj)
            finally:
                _subscriptions_lock.release()

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
        _subscribtions.append((topic, callbackObj))
    finally:
        _subscriptions_lock.release()

def _internalsubscribe_to(topic, callbackObj):
    """
    subscribe wthout using the lock or adding the topic/callback to the internal list.
    :param topic:
    :param callbackObj:function to call
    :return:
    """
    queue = channel.queue_declare(exclusive=True)
    queue_name = queue.method.queue
    channel.queue_bind(exchange='outbound', queue=queue_name, routing_key=topic)
    channel.basic_qos(prefetch_count=1)  # make certain that we only receive 1 message at a time
    channel.basic_consume(callbackObj.callback, queue=queue_name, no_ack=True)
    logging.info("subscribed to {}".format(topic))