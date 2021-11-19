import logging

from kombu.transport.virtual import base

logger = logging.getLogger(__name__)

class SQSMessage(base.Message):
    def change_visibility_timeout(self, new_timeout):
        if self.channel is None:
            raise self.MessageStateError(
                'This message does not have a receiving channel')
        self.channel.change_visibility_timeout(self.delivery_tag, new_timeout)
