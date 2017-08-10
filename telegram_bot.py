import json

import logging
import urllib
from base64 import urlsafe_b64encode, b32encode, b16encode, urlsafe_b64decode

from multiprocessing import Process

import telegram
from bson import ObjectId
from flask import Flask, request, Response, Blueprint
from pymongo import MongoClient

from bot_utils import set_webhook, make_bot_app

import time
import sched

try:
    from yaml import CLoader as Loader, CDumper as Dumper, load
except ImportError:
    from yaml import Loader, Dumper, load

telegram_bp = Blueprint('telegram_bp', __name__)


# --- REST block START ---
@telegram_bp.route('/', methods=['POST'])
def incoming_from_telegram():
    telegram_bp.logger.debug('received request. post data: {0}'.format(request.get_data()))

    event_handler_queue = telegram_bp.event_handler_queue
    data = json.loads(request.get_data())
    # --- request handling block START ---
    # retrieve the message in JSON and then transform it to Telegram object
    update = telegram.Update.de_json(request.get_json(force=True), telegram_bp.bot)

    mongo_config = telegram_bp.config['mongo']
    client = MongoClient(
        mongo_config['urlPattern'].format(mongo_config['user'], mongo_config['password'], mongo_config['host']))
    db = client[mongo_config['admsgConfigDB']]
    user_tracker_db = client[mongo_config['userTracker']['db']]

    if update.message != None:
        chat_id = update.message.chat_id

        if '/start' in update.message.text and update.message.text.replace('/start', ''):
            channel_id, campaign_id = urlsafe_b64decode(str(update.message.text).replace('/start ', '')).split(';')
            data['channel_id'] = channel_id
            data['campaign_id'] = campaign_id

        campaign = db.Campaign.find_one({'_id': campaign_id})
        link = campaign['link']

        map_id = ObjectId()
        user_tracker_db.map.update(
            {"_id": map_id,
             'channel_id': channel_id,
             'campaign_id': campaign_id,
             'user_id': update.message.from_user.id,
             'link': urllib.quote(link, safe='')
             },
            {'$currentDate': {
                'ts': {'$type': 'timestamp'}
            }
            },
            {'upsert': True}
        )

        text = campaign['text'] + ' ' + \
               telegram_bp.config['bot']['trackerUrlPattern'] \
                   .format(map_id)
        # Telegram understands UTF-8, so encode text for unicode compatibility


        # repeat the same message back (echo)
        telegram_bp.bot.sendMessage(chat_id=chat_id, text=text.encode('utf-8'))
        # --- simple request handling block END ---

    event_handler_queue.put_nowait(('raw_data', (mongo_config['bot']['collection']['telegram'], json.dumps(data))))
    client.close()
    return Response(status=200)


# --- REST block END ---


def make_telegram_app(config, event_handler_queue, bot, url):
    return make_bot_app(config, telegram_bp, bot.setWebhook, bot, url, event_handler_queue)


def make_telegram_bot(bot_auth_token):
    return telegram.Bot(token=bot_auth_token)


def make_telegram_deep_link(deeplink_pattern, bot_id, channel_id, campaign_id):
    return deeplink_pattern.format(bot_id, urlsafe_b64encode(channel_id + ';' + campaign_id))
