import json

import logging

from multiprocessing import Process

import telegram
from flask import Flask, request, Response, Blueprint

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
    event_handler_queue.put_nowait(('raw_data', request.get_data()))
    # --- request handling block START ---
    # retrieve the message in JSON and then transform it to Telegram object
    update = telegram_bp.bot.Update.de_json(request.get_json(force=True))

    chat_id = update.message.chat.id

    # Telegram understands UTF-8, so encode text for unicode compatibility
    text = update.message.text.encode('utf-8')

    # repeat the same message back (echo)
    telegram_bp.bot.sendMessage(chat_id=chat_id, text=text)
    # --- simple request handling block END ---

    return Response(status=200)


# --- REST block END ---


def make_viber_bot_app(logger_config, event_handler_queue, bot_auth_token, url):

    bot = telegram.Bot(token=bot_auth_token)

    return make_bot_app(logger_config, telegram_bp, bot.setWebhook, bot, url, event_handler_queue)
