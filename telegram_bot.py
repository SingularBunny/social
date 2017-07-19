import json

import logging

from multiprocessing import Process

from flask import Flask, request, Response, Blueprint

from viberbot import Api
from viberbot.api import messages
from viberbot.api.bot_configuration import BotConfiguration
from viberbot.api.messages.text_message import TextMessage
from viberbot.api.viber_requests import ViberConversationStartedRequest
from viberbot.api.viber_requests import ViberFailedRequest
from viberbot.api.viber_requests import ViberMessageRequest
from viberbot.api.viber_requests import ViberSubscribedRequest
from viberbot.api.viber_requests import ViberUnsubscribedRequest

import time
import sched

try:
    from yaml import CLoader as Loader, CDumper as Dumper, load
except ImportError:
    from yaml import Loader, Dumper, load

telegram_bp = Blueprint('telegram_bp', __name__)

# --- REST block START ---
@telegram_bp.route('/', methods=['POST'])
def incoming_from_viber():
    telegram_bp.logger.debug('received request. post data: {0}'.format(request.get_data()))

    event_handler_queue = telegram_bp.event_handler_queue
    event_handler_queue.put_nowait(('raw_data', request.get_data()))
    # --- request handling block START ---
    # retrieve the message in JSON and then transform it to Telegram object
    update = telegram_bp.telegram.Update.de_json(request.get_json(force=True))

    chat_id = update.message.chat.id

    # Telegram understands UTF-8, so encode text for unicode compatibility
    text = update.message.text.encode('utf-8')

    # repeat the same message back (echo)
    telegram_bp.telegram.sendMessage(chat_id=chat_id, text=text)
    # --- simple request handling block END ---

    return Response(status=200)


def set_webhook(logger_config, viber, url):
    logging.config.dictConfig(logger_config)
    logger = logging.getLogger('Webhook')
    logger.debug('{0} started'.format('WebhookSetter'))
    while True:
        try:
            viber.set_webhook(url)
            break
        except Exception as e:
            logger.debug(e)


# --- REST block END ---


def make_viber_bot(logger_config, event_handler_queue, bot_name, bot_avatar, bot_auth_token, url):

    logging.config.dictConfig(logger_config)
    logger = logging.getLogger()

    viber = Api(BotConfiguration(
        name=bot_name,
        avatar=bot_avatar,
        auth_token=bot_auth_token
    ))

    app = Flask(__name__)
    app.register_blueprint(viber_bp)

    # init webhooks
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(5, 1, set_webhook, (logger_config, viber, url))
    webhook_setter = Process(name='Webhook Setter',
                target=scheduler.run)
    webhook_setter.daemon = True

    app.logger = logger
    app.webhook_setter = webhook_setter

    viber_bp.logger = logger
    viber_bp.viber = viber
    viber_bp.event_handler_queue = event_handler_queue
    return app
