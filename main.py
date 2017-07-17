import json

import logging
from pymongo import MongoClient

from multiprocessing import Process, Queue, Event

from flaskrun import flaskrun
from logging_utils import listener_process
from viber_bot import make_viber_bot

try:
    from yaml import CLoader as Loader, CDumper as Dumper, load
except ImportError:
    from yaml import Loader, Dumper, load

# TODO: 1. Tests coverage.
# TODO: 2. Move configuration to yaml.

# logger
SIMPLE_LOGGER_CONFIG = 'logging_utils/simple_logger_config.yaml'

# Bot
BOT_NAME = 'PythonBot',
BOT_AVATAR = 'http://cs9.pikabu.ru/images/big_size_comm/2017-01_7/1485863230198053474.jpg',
BOT_AUTH_TOKEN = '46035a5801f49064-54a5b815877ccb4d-176654aa2704e14a'
BOT_WEBHOOK_URL = 'https://admsg.ru:{}/'
BOT_WEBHOOK_PORT = 8443

# Mongo
MONGO_PREFIX = 'mongodb://'
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'viber'

# Application
DEBUG = True
PATH_TO_CRT = 'certificates/server.crt'
PATH_TO_KEY = 'certificates/server.key'

# Process names
EVENT_PROCESSOR = 'EventProcessor'
STATS_MAINTAINER = 'StatsStorer'

# dictionary where key is an class of Viber and value is a key from event_queues_dict
subscribers_dict = {'raw_data': (STATS_MAINTAINER,)}

# incoming queues of all processes
event_queues_dict = {EVENT_PROCESSOR: Queue(),
                     STATS_MAINTAINER: Queue()}


def process_events(logger_config, event_queues_dict, subscribers_dict):
    """
    Publisher-subscriber pattern implementation.
    
    :param logger_config: logger configuration.
    :param event_queues_dict: dictionary with incoming queues of processes.
    :param subscribers_dict: Request class is key and process name is value.
    """
    logging.config.dictConfig(logger_config)
    logger = logging.getLogger(EVENT_PROCESSOR)
    logger.debug('{0} started'.format(EVENT_PROCESSOR))
    event_handler_queue = event_queues_dict.get(EVENT_PROCESSOR)
    while True:
        key, event = event_handler_queue.get()
        for dict_key, subscribers in subscribers_dict.iteritems():
            if key == dict_key:
                for subscriber in subscribers:
                    logger.debug('Process event: {0} to subscriber: {1}'.format(event, subscriber))
                    event_queues_dict.get(subscriber).put_nowait(event)


def maintain_statistics(logger_config, queue):
    """
    Stores statistics.
    
    :param logger_config: logger configuration.
    :param queue: incoming queue.
    """
    # TODO For User Profiles possible to use DBRef
    logging.config.dictConfig(logger_config)
    logger = logging.getLogger(STATS_MAINTAINER)
    logger.debug('{0} started'.format(STATS_MAINTAINER))
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    while True:
        event = queue.get()
        logger.debug('Store event: {0} to Mongo'.format(event))
        db.events.insert_one(json.loads(event))


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
    # pydevd.settrace('109.195.27.157', port=5123, stdoutToServer=True, stderrToServer=True, suspend=False)

    # --- init logger START ---
    logger_queue = Queue()
    with open(SIMPLE_LOGGER_CONFIG, 'r') as logger_config:
        config = load(logger_config, Loader=Loader)
        config_worker = config.get('worker')  # logger config for workers.
        config_worker['handlers']['queue']['queue'] = logger_queue
        config_listener = config.get('listener')

    stop_event = Event()
    lp = Process(target=listener_process,
                 name='listener',
                 args=(logger_queue, stop_event, config_listener))
    lp.daemon = True
    lp.start()

    logging.config.dictConfig(config_worker)
    logger = logging.getLogger()
    # --- init logger START ---

    init_mongo()

    event_processor = Process(name=EVENT_PROCESSOR,
                              target=process_events,
                              args=(config_worker,
                                    event_queues_dict,
                                    subscribers_dict
                                    ))
    event_processor.daemon = True
    event_processor.start()

    stats_maintainer = Process(name=STATS_MAINTAINER,
                               target=maintain_statistics,
                               args=(config_worker,
                                     event_queues_dict.get(STATS_MAINTAINER)
                                     ))
    stats_maintainer.daemon = True
    stats_maintainer.start()

    app = make_viber_bot(config_worker, event_queues_dict[EVENT_PROCESSOR], BOT_NAME, BOT_AVATAR, BOT_AUTH_TOKEN,
                         BOT_WEBHOOK_URL.format(BOT_WEBHOOK_PORT))
    app.webhook_setter.start()

    flaskrun(app, default_host='0.0.0.0', default_port=BOT_WEBHOOK_PORT)
