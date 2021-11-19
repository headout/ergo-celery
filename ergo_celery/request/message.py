import logging

from kombu.transport.virtual import base

logger = logging.getLogger(__name__)
class SQSMessage(base.Message):
    def change_visibility_timeout(self, new_timeout, errors=[]):
        if self.channel is None:
            raise self.MessageStateError(
                'This message does not have a receiving channel')
        try:
            self.channel.change_visibility_timeout(self.delivery_tag, new_timeout)
        except errors as ex:
            logger.warn('Unable to change visibility timeout', exc_info=ex)
        else:
            logger.info(f'Task "{self.delivery_tag}" changed visibility timeout to {new_timeout}')
