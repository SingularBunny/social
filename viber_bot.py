import json

from base64 import urlsafe_b64encode, urlsafe_b64decode

from bson import ObjectId
from flask import request, Response, Blueprint
from pymongo import MongoClient

from bot_utils import make_bot_app
from viberbot import Api

from viberbot.api.bot_configuration import BotConfiguration
from viberbot.api.messages.text_message import TextMessage
from viberbot.api.viber_requests import ViberConversationStartedRequest

try:
    from yaml import CLoader as Loader, CDumper as Dumper, load
except ImportError:
    from yaml import Loader, Dumper, load

viber_bp = Blueprint('viber_bp', __name__)


# --- REST block START ---
@viber_bp.route('/', methods=['POST'])
def incoming_from_viber():
    viber_bp.logger.debug('received request. post data: {0}'.format(request.get_data()))

    data = json.loads(request.get_data())
    # --- request handling block START ---
    # retrieve the message in JSON and then transform it to Telegram object
    viber_request = viber_bp.bot.parse_request(request.get_data())

    mongo_config = viber_bp.config['mongo']
    client = MongoClient(
        mongo_config['urlPattern'].format(mongo_config['user'], mongo_config['password'], mongo_config['host']))
    db = client[mongo_config['admsgConfigDB']]
    user_tracker_db = client[mongo_config['userTracker']['db']]

    if isinstance(viber_request, ViberConversationStartedRequest):
        channel_id, campaign_id = urlsafe_b64decode(viber_request.context).split(';')
        data['channel_id'] = channel_id
        data['campaign_id'] = campaign_id

        campaign = db.Campaign.find_one({'_id': campaign_id})
        link = campaign['link']

        map_id = ObjectId()
        user_tracker_db.map.update(
            {"_id": map_id,
             'channel_id': channel_id,
             'campaign_id': campaign_id,
             'user_id': viber_request.user.id,
             'link': link
             },
            {'$currentDate': {
                'ts': {'$type': 'timestamp'}
            }
            },
            upsert=True
        )

        text = campaign['text'] + ' ' + \
               viber_bp.config['bot']['trackerUrlPattern'] \
                   .format(map_id)
        # Telegram understands UTF-8, so encode text for unicode compatibility


        # repeat the same message back (echo)
        # TODO CHECK THIS CALL
        viber_bp.bot.send_messages(viber_request.user.id, [
            TextMessage(text=text)
        ])
        # --- simple request handling block END ---

    event_handler_queue = viber_bp.event_handler_queue
    event_handler_queue.put_nowait(('raw_data', (mongo_config['bot']['collection']['viber'], request.get_data())))
    client.close()
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


def make_viber_app(config, event_handler_queue, bot, url):
    return make_bot_app(config, viber_bp, bot.set_webhook, bot, url, event_handler_queue)


def make_viber_bot(bot_name, bot_avatar, bot_auth_token):
    return Api(BotConfiguration(
        name=bot_name,
        avatar=bot_avatar,
        auth_token=bot_auth_token
    ))


def make_viber_deep_link(deeplink_pattern, bot_id, channel_id, campaign_id):
    return deeplink_pattern.format(bot_id, urlsafe_b64encode(channel_id + ';' + campaign_id))
