import json

import logging

import time
from pymongo import MongoClient

from multiprocessing import Process, Queue, Event

from flaskrun import flaskrun
from logging_utils import listener_process
from telegram_bot import make_telegram_app, make_telegram_bot, make_telegram_deep_link
from viber_bot import make_viber_app, make_viber_deep_link

try:
    from yaml import CLoader as Loader, CDumper as Dumper, load
except ImportError:
    from yaml import Loader, Dumper, load

# TODO: 1. Tests coverage.
# TODO: 2. Move configuration to yaml.
# user_id
# campaign_id
# channel_id

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

CHANNEL_TYPE_VIBER = 'viber'
CHANNEL_TYPE_TELEGRAM ='telegram'

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


def maintain_statistics(config, queue):
    """
    Stores statistics.

    :param logger_config: logger configuration.
    :param queue: incoming queue.
    """
    # TODO For User Profiles possible to use DBRef
    logger_config = config['loggerConfig']
    mongo_config = config['mongo']

    logging.config.dictConfig(logger_config)
    logger = logging.getLogger(STATS_MAINTAINER)
    logger.debug('{0} started'.format(STATS_MAINTAINER))
    client = MongoClient(mongo_config['urlPattern'].format(mongo_config['user'], mongo_config['password'], mongo_config['host']))
    db = client[mongo_config['bot']['db']]
    while True:
        collection, event = queue.get()
        logger.debug('Store event: {0} to Mongo'.format(event))
        db[collection].insert_one(json.loads(event))
    client.close()


def run_bots(config, stop_event):
    logger_config = config['loggerConfig']
    mongo_config = config['mongo']
    bot_config = config['bot']

    logging.config.dictConfig(logger_config)
    logger = logging.getLogger(STATS_MAINTAINER)
    logger.debug('{0} started'.format(BOT_RUNNER))

    apps = {}
    bots = {}
    ports = {}

    flag = True
    while True:
        if stop_event.is_set():
            break

        if flag:
            # run bots
            channel_id = 'AjhwTEhd11'
            token = '435537512:AAEoRhCOg3oW0FgyzxnhC-8bD-WwCoi0D6E'
            chat_id = '@cannabusiness'
            port = 8443
            channel_type = 'telegram'

            if token not in apps:
                # app = make_viber_bot_app(config_worker, event_queues_dict[EVENT_PROCESSOR], bot_name, avatar, token,
                #                          BOT_WEBHOOK_URL.format(port))
                bot = make_telegram_bot(token)
                bots[channel_id] = bot

                app = make_telegram_app(config, event_queues_dict[EVENT_PROCESSOR], bot,
                                        bot_config['webhookUrl'].format(port))
                app.webhook_setter.start()

                app_process = Process(name='',
                                      target=flaskrun,
                                      args=(app, '0.0.0.0', port, config['pathToCrt'], config['pathToKey']))
                app_process.daemon = True
                app_process.start()
                apps[channel_id] = bot
                ports[channel_id] = port

            # start campaigns
            for campaign in get_campaigns(mongo_config, channel_id):
                text = campaign['text']
                campaign_id = campaign['campaign_id']
                deep_link = make_telegram_deep_link(bot_config['deeplink']['telegram'],
                                                    bot.username,
                                                    channel_id,
                                                    campaign_id) \
                    if channel_type == CHANNEL_TYPE_TELEGRAM \
                    else make_viber_deep_link if channel_type == CHANNEL_TYPE_VIBER else None

                assert (campaign_id is not None and text is not None)

                text += ' ' + deep_link

                bots[channel_id].send_message(chat_id=chat_id, text=text.encode('utf-8'))
            flag = False
            time.sleep(30)

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
         {'$match': {'status': {'$ne': 'started'}}},
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
    client.close()
    return list(cur)


def mark_campaign_as_started(mongo_config, campaign_id):
    client = MongoClient(
        mongo_config['urlPattern'].format(mongo_config['user'], mongo_config['password'], mongo_config['host']))
    db = client[mongo_config['admsgConfigDB']]
    db.Campaign.update({'_id': campaign_id}, {'$set': {'status': 'started'}})
    client.close()


def init_mongo(mongo_config):
    """
    Init Mongo indexes.
    """
    logger.debug('Init Mongo')
    client = MongoClient(mongo_config['urlPattern'].format(mongo_config['user'], mongo_config['password'], mongo_config['host']))
    db = client[mongo_config['bot']['db']]
    for collection in mongo_config['bot']['collection'].keys():
        db[mongo_config['bot']['collection'][collection]].create_index([('$**', 'text')])
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
    config['loggerConfig'] = config_worker

    stop_event = Event()
    lp = Process(target=listener_process,
                 name='listener',
                 args=(logger_queue, stop_event, config_listener))
    lp.daemon = True
    lp.start()

    logging.config.dictConfig(config_worker)
    logger = logging.getLogger()
    logger.debug('hello')
    # --- init logger START ---

    #init_mongo(config['mongo'])

    event_processor = Process(name=EVENT_PROCESSOR,
                              target=process_events,
                              args=(config_worker,
                                    event_queues_dict,
                                    subscribers_dict
                                    ))
    event_processor.start()

    stats_maintainer = Process(name=STATS_MAINTAINER,
                               target=maintain_statistics,
                               args=(config,
                                     event_queues_dict.get(STATS_MAINTAINER)
                                     ))
    stats_maintainer.start()

    bot_runner = Process(name=BOT_RUNNER,
                         target=run_bots,
                         args=(config,
                               stop_event
                               ))
    bot_runner.start()
    bot_runner.join()