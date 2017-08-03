import json

import logging
from pymongo import MongoClient

from multiprocessing import Process, Queue, Event

from flaskrun import flaskrun
from logging_utils import listener_process
from telegram_bot import make_telegram_app, make_telegram_bot
from viber_bot import make_viber_app

try:
    from yaml import CLoader as Loader, CDumper as Dumper, load
except ImportError:
    from yaml import Loader, Dumper, load

# TODO: 1. Tests coverage.
# TODO: 2. Move configuration to yaml.
# user_id
# campaign_id
# channel_id

# logger
SIMPLE_LOGGER_CONFIG = 'logging_utils/simple_logger_config.yaml'

# bots
BOT_WEBHOOK_URL = 'https://admsg.ru:{}/'
BOT_WEBHOOK_PORT = 8443
TRACKER_URL_PATTERN = 'http://admsg.ru:8083/tracker?targetUrl={}&channel_id={}&campaign_id={}&user_id={}'
# Viber Bot
VIBER_BOT_NAME = 'PythonBot'
VIBER_BOT_AVATAR = 'http://cs9.pikabu.ru/images/big_size_comm/2017-01_7/1485863230198053474.jpg'
VIBER_BOT_AUTH_TOKEN = '46035a5801f49064-54a5b815877ccb4d-176654aa2704e14a'

# Telegram Bot
TELEGRAM_BOT_AUTH_TOKEN = '46035a5801f49064-54a5b815877ccb4d-176654aa2704e14a'

# Mongo
MONGO_PREFIX = 'mongodb://'
MONGO_URL_PATTERN = 'mongodb://{}:{}@{}'
MONGO_HOST = 'admsg.ru'
MONGO_USER = 'admsg'
MONGO_PASSWORD = 'gfhjkm1lkz2flv0pu'
MONGO_ADMSG_CONFIG_DB = 'admsg_config'
MONGO_VIBER_DB = 'viber'
DB_NAMES = [MONGO_VIBER_DB]

# Application
DEBUG = True
PATH_TO_CRT = 'certificates/server.crt'
PATH_TO_KEY = 'certificates/server.key'

# --- Processes block START ---

# Process names
EVENT_PROCESSOR = 'EventProcessor'
STATS_MAINTAINER = 'StatsStorer'
BOT_RUNNER = 'BotRunner'

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
    client = MongoClient(MONGO_URL_PATTERN.format(MONGO_USER, MONGO_PASSWORD, MONGO_HOST))
    db = client[MONGO_VIBER_DB]
    while True:
        event = queue.get()
        logger.debug('Store event: {0} to Mongo'.format(event))
        db.events.insert_one(json.loads(event))


def run_bots(application_config, stop_event):
    logging.config.dictConfig(application_config['loggerConfig'])
    logger = logging.getLogger(STATS_MAINTAINER)
    logger.debug('{0} started'.format(BOT_RUNNER))

    apps = {}
    bots = {}
    ports = {}

    while True:
        if stop_event.is_set():
            break

        # run bots
        channel_id = 'AjhwTEhd11'
        token = '435537512:AAEoRhCOg3oW0FgyzxnhC-8bD-WwCoi0D6E'
        chat_id = ''
        port = BOT_WEBHOOK_PORT

        if token not in apps:
            # app = make_viber_bot_app(config_worker, event_queues_dict[EVENT_PROCESSOR], bot_name, avatar, token,
            #                          BOT_WEBHOOK_URL.format(port))
            bot = make_telegram_bot(token)
            bots[channel_id] = bot

            app = make_telegram_app(config_worker, event_queues_dict[EVENT_PROCESSOR], bot,
                                    BOT_WEBHOOK_URL.format(port))
            app.webhook_setter.start()

            app_process = Process(name='',
                                  target=flaskrun,
                                  args=(app, '0.0.0.0', port, PATH_TO_CRT, PATH_TO_KEY))
            app_process.daemon = True
            app_process.start()
            apps[channel_id] = bot
            ports[channel_id] = port

        # start campaigns
        for campaign in get_campaigns(application_config['mongo'], channel_id):
            text = campaign['text']
            campaign_id = campaign['campaign_id']
            deep_link = make_deep_link(campaign_id)

            assert (campaign_id is not None and text is not None)

# --- Processes block END ---

def get_channels(mongo_config):
    pass


# TODO add filter for started campaigns
def get_campaigns(mongo_config, channel_id):
    client = MongoClient(
        mongo_config['urlPattern'].format(mongo_config['user'], mongo_config['password'], mongo_config['host']))
    db = client[mongo_config['admsgConfigDB']]
    cur = db.Channel.aggregate(
        [{'$match': {'_id': channel_id}},
         {'$project': {'id': {'$concat': ['Channel$', '$_id']}, 'name': 1}},
         {'$lookup': {'from': 'CampaignChannels', 'localField': 'id', 'foreignField': '_p_channel',
                      'as': 'channel_campaigns'}},
         {'$unwind': '$channel_campaigns'},
         {'$project': {'name': 1,
                       'campaign': {'$substr': ['$channel_campaigns._p_campaign', len('Campaign$'), -1]}}},
         {'$lookup': {'from': 'Campaign', 'localField': 'campaign', 'foreignField': '_id', 'as': 'campaigns'}},
         {'$unwind': '$campaigns'},
         {'$project': {'name': 1, 'channel': 1, 'campaign_id': '$campaigns._id', 'text': '$campaigns.text',
                       'link': '$campaigns.link'}}])
    return list(cur)


def make_deep_link(campaign_id):
    pass


def mark_campaign_as_started(campaign_id):
    pass


def init_mongo():
    """
    Init Mongo indexes.
    """
    logger.debug('Init Mongo')
    client = MongoClient(MONGO_URL_PATTERN.format(MONGO_USER, MONGO_PASSWORD, MONGO_HOST))

    for db_name in DB_NAMES:
        db = client[db_name]
        db.events.create_index([('$**', 'text')])
        client.close()


if __name__ == '__main__':
    # debug support
    # pydevd.settrace('109.195.27.157', port=5123, stdoutToServer=True, stderrToServer=True, suspend=False)

    # --- init logger START ---
    with open('configuration/config.yaml', 'r') as confFile:
        config = load(confFile, Loader=Loader)

    logger_queue = Queue()
    with open(config['loggerConfig'], 'r') as logger_config:
        logger_config = load(logger_config, Loader=Loader)
        config_worker = logger_config['worker']  # logger config for workers.
        config_worker['handlers']['queue']['queue'] = logger_queue
        config_listener = logger_config['listener']

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

    bot_runner = Process(name=BOT_RUNNER,
                         target=run_bots,
                         args=(config_worker,
                               stop_event
                               ))
    bot_runner.start()
