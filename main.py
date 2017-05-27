import json

from pymongo import MongoClient

import pydevd
from multiprocessing import Process, Queue

from flask import Flask, request, Response

from viberbot.api import messages
from viberbot.api.viber_requests import ViberRequest

from viberbot import Api
from viberbot.api.bot_configuration import BotConfiguration
from viberbot.api.messages.text_message import TextMessage
from viberbot.api.viber_requests import ViberConversationStartedRequest
from viberbot.api.viber_requests import ViberFailedRequest
from viberbot.api.viber_requests import ViberMessageRequest
from viberbot.api.viber_requests import ViberSubscribedRequest
from viberbot.api.viber_requests import ViberUnsubscribedRequest

import time
import logging
import sched

# TODO: 1. Tests coverage.
# TODO: 2. Move configuration to yaml.

# Mongo
MONGO_PREFIX = 'mongodb://'
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'viber'

# Application
PORT = 8443
DEBUG = True
PATH_TO_CRT = 'certificates/server.crt'
PATH_TO_KEY = 'certificates/server.key'

# Process names
EVENT_PROCESSOR = 'EventProcessor'
STATS_MAINTAINER = 'StatsStorer'

def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

logger = init_logger()

app = Flask(__name__)
viber = Api(BotConfiguration(
    name='PythonBot',
    avatar='http://cs9.pikabu.ru/images/big_size_comm/2017-01_7/1485863230198053474.jpg',
    auth_token='46035a5801f49064-54a5b815877ccb4d-176654aa2704e14a'
))

# dictionary where key is an class of Viber and value is a key from event_queues_dict
subscribers_dict = {}

# incoming queues of all processes
event_queues_dict = {EVENT_PROCESSOR: Queue()}

# --- REST block START ---
@app.route('/', methods=['POST'])
def incoming_from_viber():
    logger.debug('received request. post data: {0}'.format(request.get_data()))

    viber_request = viber.parse_request(request.get_data())

    # --- simple request handling block START ---
    if isinstance(viber_request, ViberMessageRequest):
        message = viber_request.message()
        viber.send_messages(viber_request.sender().get_id(), [
            message
        ])
    elif isinstance(viber_request, ViberConversationStartedRequest) \
            or isinstance(viber_request, ViberSubscribedRequest) \
            or isinstance(viber_request, ViberUnsubscribedRequest):
        viber.send_messages(viber_request.user.id, [
            TextMessage(None, None, viber_request.type)
        ])
    elif isinstance(viber_request, ViberFailedRequest):
        logger.warn('client failed receiving message. failure: {0}'.format(viber_request))
    # --- simple request handling block END ---

    event_handler_queue = event_queues_dict.get(EVENT_PROCESSOR)
    event_handler_queue.put_nowait(request.get_data())

    return Response(status=200)

@app.route('/post_message/<string:admin_id>', methods=['POST'])
def post_message(admin_id):
    logger.debug('Send message request. post data: {0}'.format(request.get_data()))
    viber.post(admin_id, messages.get_message(json.loads(request.get_data())))
    return Response(status=200)

@app.route('/account_info', methods=['GET'])
def account_info():
    logger.debug('Get account info request.')
    return json.dumps(viber.get_account_info())

def set_webhook(viber):
    logger = logging.getLogger()
    logger.debug('{0} started'.format('WebhookSetter'))
    while True:
        try:
            viber.set_webhook('https://admsg.ru:{}/'.format(PORT))
            break
        except Exception as e:
            logger.debug(e)

# --- REST block END ---

# --- Processes block START ---
#
def process_events(event_queues_dict, subscribers_dict):
    """
    Publisher-subscriber pattern implementation.
    
    :param logger: logger.
    :param event_queues_dict: dictionary with incoming queues of processes.
    :param subscribers_dict: Request class is key and process name is value.
    """
    logger = init_logger()
    logger.debug('{0} started'.format(EVENT_PROCESSOR))
    event_handler_queue = event_queues_dict.get(EVENT_PROCESSOR)
    while True:
        event = event_handler_queue.get()
        for class_key, subscribers in subscribers_dict.iteritems():
            logger.debug('Check event: {0} is instance of : {1}'.format(event, class_key))
            if isinstance(event, class_key):
                for subscriber in subscribers:
                    logger.debug('Process event: {0} to subscriber: {1}'.format(event, subscriber))
                    event_queues_dict.get(subscriber).put_nowait(event)


def maintain_statistics(queue):
    """
    Stores statistics.
    
    :param logger: logger.
    :param queue: incoming queue.
    :param viber: Viber API instance.
    """
    # TODO For User Profiles possible to use DBRef
    logger = init_logger()
    logger.debug('{0} started'.format(STATS_MAINTAINER))
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    while True:
        event = queue.get()
        logger.debug('Store event: {0} to Mongo'.format(event))
        db.events.insert_one(event)
# --- Processes block END ---

def init_mongo():
    """
    Init Mongo indexes.
    """
    logger.debug('Init Mongo')
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    db.events.create_index([('$**', 'text')])
    client.close()

if __name__ == '__main__':

    # debug support
    #pydevd.settrace('109.195.27.157', port=5123, stdoutToServer=True, stderrToServer=True, suspend=False)

    init_mongo()

    event_processor = Process(name=EVENT_PROCESSOR,
                              target=process_events,
                              args=(event_queues_dict,
                                    subscribers_dict
                                    ))
    event_processor.start()

    stats_maintainer = Process(name=STATS_MAINTAINER,
                               target=maintain_statistics,
                               args=(event_queues_dict.setdefault(STATS_MAINTAINER, Queue()),))
    # make subscriptions
    subscribers_dict.setdefault(ViberRequest, []).append(STATS_MAINTAINER)
    stats_maintainer.start()

    # init webhooks
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(5, 1, set_webhook, (viber, ))
    t = Process(name='WebhookSetter',
                target=scheduler.run)
    t.start()

    logger.debug('Subscribers: {}'.format(subscribers_dict))
    logger.debug('Queues: {}'.format(event_queues_dict))
    # REST start
    context = (PATH_TO_CRT, PATH_TO_KEY)
    app.run(host='0.0.0.0', port=PORT, debug=DEBUG, ssl_context=context)
