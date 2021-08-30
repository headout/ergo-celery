import uuid
from kombu.serialization import dumps, loads
from kombu.transport.SQS import (Channel, Transport, UndefinedQueueException,
                                 logger)


class ErgoChannel(Channel):
    DEFAULT_CONTENT_TYPE = 'application/json'
    DEFAULT_CONTENT_ENCODING = 'utf-8'
    MESSAGE_ATTRIBUTES = ['ApproximateReceiveCount']

    def put_bulk(self, queue, messages, **kwargs):
        q_url = self._new_queue(queue)
        entries = [
            {
                'Id': str(idx),
                'MessageBody': dumps(msg, 'json')[2],
                'MessageGroupId': msg['taskId'],
                'MessageDeduplicationId': str(uuid.uuid4())
            }
            for idx, msg in enumerate(messages)
        ]
        logger.info(f'Request to push: {entries}')
        c = self.sqs(queue=self.canonical_queue_name(queue))
        resp = c.send_message_batch(QueueUrl=q_url, Entries=entries, **kwargs)
        return (resp.get('Successful', []), resp.get('Failed', []))

    def _to_proto1(self, orig_payload, sqs_msg):
        payload = dict()
        payload.update(orig_payload)

        # Set content type/encoding of the payload
        payload.setdefault('content-type', self.DEFAULT_CONTENT_TYPE)
        payload.setdefault('content-encoding', self.DEFAULT_CONTENT_ENCODING)
        payload['headers'] = {}  # empty headers in Proto 1

        body = payload['body'] or '{}'
        safe_kwargs = body # the body in ergo refers to function kwargs
        body = loads(body, payload['content-type'], payload['content-encoding'])
        body['task'] = sqs_msg['Attributes']['MessageGroupId']
        body['id'] = sqs_msg['MessageId']
        body['kwargs'] = loads(safe_kwargs, payload['content-type'], payload['content-encoding'])

        # Dump the payload back to string for later deserialization by the consumer
        _, _, payload['body'] = dumps(body, 'json')
        return payload

    def _message_to_python(self, message, queue_name, queue):
        payload = super()._message_to_python(message, queue_name, queue)
        sqs_msg = payload['properties']['delivery_info']['sqs_message']
        if 'headers' not in payload and sqs_msg['Attributes'].get('MessageGroupId', None):
            # Detected Ergo Protocol, so convert it to Proto 1-compatible schema
            payload = self._to_proto1(payload, sqs_msg)
        return payload

    # TODO: Better to add the FIFO queue support in Kombu itself (reminder: Raise a PR)
    def _get_from_sqs(self, queue,
                      count=1, connection=None, callback=None):
        """Retrieve and handle messages from SQS.

        Uses long polling and returns :class:`~vine.promises.promise`.
        """
        connection = connection if connection is not None else queue.connection
        if self.predefined_queues:
            if queue not in self._queue_cache:
                raise UndefinedQueueException((
                    "Queue with name '{}' must be defined in "
                    "'predefined_queues'."
                ).format(queue))
            queue_url = self._queue_cache[queue]
        else:
            queue_url = connection.get_queue_url(queue)
        attributes = self.MESSAGE_ATTRIBUTES
        # For FIFO queue, also fetch MessageGroupId attribute
        if 'fifo' in queue_url: ## TODO: BETTER IS queue.endswith('.fifo')
            attributes += ['MessageGroupId']
        return connection.receive_message(
            queue, queue_url, number_messages=count,
            attributes=attributes,
            wait_time_seconds=self.wait_time_seconds,
            callback=callback,
        )


class SQSTransport(Transport):
    Channel = ErgoChannel
