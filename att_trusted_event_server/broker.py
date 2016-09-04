__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

#connect to the broker and monitor the correct topics so that the required rules can be triggered.

import logging
import pika
import threading
from callbackObj import CallbackObj

channel = None


def connect(username, pwd, brokerName):
    """connect to the broker"""
    global channel
    credentials = pika.PlainCredentials(username, pwd)
    connection = pika.BlockingConnection(pika.ConnectionParameters(brokerName, credentials=credentials, virtual_host="att-vhost"))
    channel = connection.channel()


def process():
    """consume messages from the queue and execute the rules
    Note: this starts a new thread, so it is a none blocking call.
    """
    print(' [*] Waiting for messages.')
    mq_recieve_thread = threading.Thread(target=channel.start_consuming)
    mq_recieve_thread.start()


def subscribeTo(topic, callbackObj):
    """
    subscribe to the topic so that the callback function in the callback object will be called.
    :param topic:
    :param callback: function to call
    :return:
    """
    queue = channel.queue_declare(exclusive=True)
    queue_name = queue.method.queue
    channel.queue_bind(exchange='outbound', queue=queue_name, routing_key=topic)
    channel.basic_qos(prefetch_count=1)  # make certain that we only receive 1 message at a time
    channel.basic_consume(callbackObj.callback, queue=queue_name, no_ack=True)
    logging.info("subscribed to {}".format(topic))