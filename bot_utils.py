import logging
import sched
from multiprocessing import Process

import time
from flask import Flask


def set_webhook(logger_config, set_webhook, url):
    logging.config.dictConfig(logger_config)
    logger = logging.getLogger('Webhook')
    logger.debug('{0} started'.format('WebhookSetter'))
    while True:
        try:
            set_webhook(url)
            break
        except Exception as e:
            logger.debug(e)

def make_bot_app(logger_config, bot_blueprint, set_webhook_method, bot, url, event_handler_queue):

    logging.config.dictConfig(logger_config)
    logger = logging.getLogger()

    app = Flask(__name__)
    app.register_blueprint(bot_blueprint)

    # init webhooks
    scheduler = sched.scheduler(time.time, time.sleep)
    scheduler.enter(5, 1, set_webhook, (logger_config, set_webhook_method, url))
    webhook_setter = Process(name='Webhook Setter',
                target=scheduler.run)
    webhook_setter.daemon = True

    app.logger = logger
    app.webhook_setter = webhook_setter

    bot_blueprint.logger = logger
    bot_blueprint.bot = bot
    bot_blueprint.event_handler_queue = event_handler_queue
    return app