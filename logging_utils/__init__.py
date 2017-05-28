"""

To use multiprocessing loger main process should something like this:
>>> q = Queue()
>>> with open(LOGGER_CONFIG, 'r') as logger_config:
>>>     config = load(logger_config, Loader=Loader)
>>>     config_worker = config.get('worker')  # logger config for workers.
>>>     config_worker['queue'] = q
>>>     config_listener = config.get('listener')
>>> stop_event = Event()
>>> lp = Process(target=listener_process, name='listener',
>>>              args=(q, stop_event, config_listener))
>>> lp.start()
>>> stop_event.set()
>>> lp.join()
and inside spawned processes:
>>> logging.config.dictConfig(config)
>>> logger = logging.getLogger('ProcessName')
>>> logger.debug('message')
"""

import logging
import logging.config
import logging.handlers
from multiprocessing import Process, Queue, Event, current_process

from logging_utils import handlers

try:
    from yaml import CLoader as Loader, CDumper as Dumper, load
except ImportError:
    from yaml import Loader, Dumper, load

# config paths
LOGGER_CONFIG = 'logger_config.yaml'

class SimpleHandler:
    """
    A simple handler for logging events. It runs in the listener process and
    dispatches events to loggers based on the name in the received record,
    which then get dispatched, by the logging system, to the handlers
    configured for those loggers.
    """
    def handle(self, record):
        logger = logging.getLogger(record.name)
        # The process name is transformed just to show that it's the listener
        # doing the logging to files and console
        record.processName = '%s (for %s)' % (current_process().name, record.processName)
        logger.handle(record)

def listener_process(q, stop_event, config):
    """
    This could be done in the main process, but is just done in a separate
    process for illustrative purposes.

    This initialises logging according to the specified configuration,
    starts the listener and waits for the main process to signal completion
    via the event. The listener is then stopped, and the process exits.
    """
    logging.config.dictConfig(config)
    listener = handlers.QueueListener(q, SimpleHandler())
    listener.start()
    stop_event.wait()
    listener.stop()
