from time import time

from celery.worker.request import Request
from kombu.transport.SQS import Channel

MAX_VISIBILITY_TIMEOUT = 43199

class SQSRequest(Request):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._attempt = 1
        self.init_visible_timeout = self.app.conf.broker_transport_options.get('visibility_timeout', Channel.default_visibility_timeout)

    def need_more_exec_time(self):
        return (time() - self.time_start) >= 0.75 * (self._attempt * self.init_visible_timeout)

    def increase_visibility_timeout(self, new_timeout):
        pass

    def ping_message(self):
        if self.need_more_exec_time():
            self._attempt += 1
            self.increase_visibility_timeout(
                min(self._attempt * self.init_visible_timeout, MAX_VISIBILITY_TIMEOUT)
            )
