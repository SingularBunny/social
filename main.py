import pydevd
from multiprocessing import Process, Queue

from flask import Flask, request, Response
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

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__)
viber = Api(BotConfiguration(
    name='PythonBot',
    avatar='http://cs9.pikabu.ru/images/big_size_comm/2017-01_7/1485863230198053474.jpg',
    auth_token='46035a5801f49064-54a5b815877ccb4d-176654aa2704e14a'
))

# dictionary where key is an class of Viber and value is a key from event_queues_dict
subscribers_dict = {}

# incoming queues of all processes
event_queues_dict = {}


@app.route('/', methods=['POST'])
def incoming():
    logger.debug("received request. post data: {0}".format(request.get_data()))

    viber_request = viber.parse_request(request.get_data())

    subscriptions = []  # contains instances of classes on which other processes are signed

    # --- simple request handling bloc start ---
    if isinstance(viber_request, ViberMessageRequest):
        message = viber_request.get_message()
        viber.send_messages(viber_request.get_sender().get_id(), [
            message
        ])
    elif isinstance(viber_request, ViberConversationStartedRequest) \
            or isinstance(viber_request, ViberSubscribedRequest) \
            or isinstance(viber_request, ViberUnsubscribedRequest):
        viber.send_messages(viber_request.get_user().get_id(), [
            TextMessage(None, None, viber_request.get_event_type())
        ])
    elif isinstance(viber_request, ViberFailedRequest):
        logger.warn("client failed receiving message. failure: {0}".format(viber_request))
    # --- simple request handling bloc end ---

    # event processor cycle
    for subscription in subscriptions:
        for class_key, subscribers in subscribers_dict.iteritems():
            if isinstance(subscription, class_key):
                for subscriber in subscribers:
                    event_queues_dict.get(subscriber).put_nowait(subscription)

    return Response(status=200)


def set_webhook(viber):
    viber.set_webhook('https://admsg.ru:8443/')


def botify(queue, viber):
    # all bot logic should be implemented here
    pass


if __name__ == "__main__":
    # debug support
    pydevd.settrace('admsg.ru', port=5123, stdoutToServer=True, stderrToServer=True)

    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(5, 1, set_webhook, (viber,))
    t = Process(target=scheduler.run)
    t.start()

    viber_bot = Process(name=viber.name(),
                        target=botify,
                        args=(event_queues_dict.setdefault(viber.name(), Queue()), viber))
    # make subscriptions
    # subscribers_dict.setdefault(class, []).append(value)
    viber_bot.start()

    context = ('certificates/server.crt', 'certificates/server.key')
    app.run(host='0.0.0.0', port=8443, debug=True, ssl_context=context)
