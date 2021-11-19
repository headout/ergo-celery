import logging

from kombu.transport.virtual import base

logger = logging.getLogger(__name__)

class SQSMessage(base.Message):
    def change_visibility_timeout(self, new_timeout, task_str, errors=[]):
        if self.channel is None:
            raise self.MessageStateError(
                'This message does not have a receiving channel')
        try:
            self.channel.change_visibility_timeout(self.delivery_tag, new_timeout)
        except errors as ex:
            logger.warn(f'Unable to change visibility timeout of {task_str}', exc_info=ex)
        except Exception as ex:
            logger.exception(f'Unable to change visibility timeout of {task_str}')
        else:
            logger.info(f'Task "{task_str}" changed visibility timeout to {new_timeout}')
