import logging
import threading
from time import time

from botocore.exceptions import ClientError
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
        duration = time() - self.time_start
        logger.debug(f'PING {self.humaninfo()}! Duration: {duration}')
        return duration >= 0.50 * (self._attempt * self.init_visible_timeout)

    def increase_visibility_timeout(self, new_timeout, worker):
        task_str = self.humaninfo()
        try:
            self.message.change_visibility_timeout(new_timeout)
        except self._connection_errors as e:
            logger.warn(f'Unable to change visibility timeout of {task_str}', exc_info=e)
        except ClientError as e:
            # maybe expired? cancel request
            logger.warn(f'Unable to change visibility timeout of {task_str}', exc_info=e)
            if 'receipt handle has expired' in e.response.get('Error', {}).get('Message', 'Unknown'):
                self.cancel(worker.pool)
        except Exception:
            logger.exception(f'Unable to change visibility timeout of {task_str}')
        else:
            logger.info(f'Task "{task_str}" changed visibility timeout to {new_timeout}')

    def ping_message(self, worker):
        if self._lock.acquire(blocking=False):
            try:
                if self.need_more_exec_time():
                    logger.info(f'Task "{self.humaninfo()}" still pending (attempt {self._attempt}). Increasing visibility timeout...')
                    self._attempt += 1
                    self.increase_visibility_timeout(
                        min(self._attempt * self.init_visible_timeout, MAX_VISIBILITY_TIMEOUT),
                        worker
                    )
            finally:
                self._lock.release()
