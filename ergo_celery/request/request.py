import logging
import threading
from time import time

from celery.worker.request import Request
from kombu.transport.SQS import Channel

MAX_VISIBILITY_TIMEOUT = 43199

logger = logging.getLogger(__name__)

class SQSRequest(Request):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = threading.Lock()
        self._attempt = 1
        self.init_visible_timeout = self.app.conf.broker_transport_options.get('visibility_timeout', Channel.default_visibility_timeout)

    def need_more_exec_time(self):
        return (time() - self.time_start) >= 0.75 * (self._attempt * self.init_visible_timeout)

    def increase_visibility_timeout(self, new_timeout):
        self.message.change_visibility_timeout(new_timeout, errors=self._connection_errors)

    def ping_message(self):
        if self._lock.acquire(blocking=False):
            try:
                if self.need_more_exec_time():
                    logger.info(f'Task "{self.message.delivery_tag}" still pending (attempt {self._attempt}). Increasing visibility timeout...')
                    self.increase_visibility_timeout(
                        min(self._attempt * self.init_visible_timeout, MAX_VISIBILITY_TIMEOUT)
                    )
                    self._attempt += 1
            finally:
                self._lock.release()
