import json

import logging
from base64 import urlsafe_b64encode

from multiprocessing import Process

from flask import Flask, request, Response, Blueprint

from bot_utils import set_webhook, make_bot_app
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

viber_bp = Blueprint('viber_bp', __name__)


# --- REST block START ---
@viber_bp.route('/', methods=['POST'])
def incoming_from_viber():
    viber_bp.logger.debug('received request. post data: {0}'.format(request.get_data()))

    viber_request = viber_bp.bot.parse_request(request.get_data())

    # --- simple request handling block START ---
    if isinstance(viber_request, ViberMessageRequest):
        message = viber_request.message
        viber_bp.bot.send_messages(viber_request.sender.id, [
            message
        ])
    elif isinstance(viber_request, ViberConversationStartedRequest) \
            or isinstance(viber_request, ViberSubscribedRequest) \
            or isinstance(viber_request, ViberUnsubscribedRequest):
        viber_bp.bot.send_messages(viber_request.user.id, [
            TextMessage(None, None, viber_request.type)
        ])
    elif isinstance(viber_request, ViberFailedRequest):
        viber_bp.logger.warn('client failed receiving message. failure: {0}'.format(viber_request))
    # --- simple request handling block END ---

    event_handler_queue = viber_bp.event_handler_queue
    event_handler_queue.put_nowait(('raw_data', request.get_data()))
    return Response(status=200)


# @viber_bp.route('/post_message/<string:admin_id>', methods=['POST'])
# def post_message(admin_id):
#     viber_bp.logger.debug('Send message request. post data: {0}'.format(request.get_data()))
#     viber_bp.bot.post(admin_id, messages.get_message(json.loads(request.get_data())))
#     return Response(status=200)
#
#
# @viber_bp.route('/account_info', methods=['GET'])
# def account_info():
#     viber_bp.logger.debug('Get account info request.')
#     return json.dumps(viber_bp.bot.get_account_info())


# --- REST block END ---


def make_viber_app(logger_config, event_handler_queue, bot, url):
    return make_bot_app(logger_config, viber_bp, bot.set_webhook, bot, url, event_handler_queue)


def make_viber_bot(bot_name, bot_avatar, bot_auth_token):
    return Api(BotConfiguration(
        name=bot_name,
        avatar=bot_avatar,
        auth_token=bot_auth_token
    ))


def make_viber_deep_link(deeplink_pattern, bot_id, channel_id, campaign_id):
    return deeplink_pattern.format(bot_id, urlsafe_b64encode(channel_id + ';' + campaign_id))
