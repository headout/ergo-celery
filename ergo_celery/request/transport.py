import uuid
from json.decoder import JSONDecodeError
from queue import Empty

from botocore.exceptions import ClientError
from kombu.asynchronous.aws.sqs.message import AsyncMessage
from kombu.serialization import dumps, loads
from kombu.transport import SQS

from ergo_celery.request.message import SQSMessage

logger = SQS.logger


class ErgoChannel(SQS.Channel):
    Message = SQSMessage
    
    DEFAULT_CONTENT_TYPE = 'application/json'
    DEFAULT_CONTENT_ENCODING = 'utf-8'
    MESSAGE_ATTRIBUTES = ['ApproximateReceiveCount']

    def basic_reject(self, delivery_tag, requeue=False):
        logger.info(f'Rejecting message "{delivery_tag}"')
        if not requeue:
            self.basic_ack(delivery_tag, requeue)
        else:
            super().basic_reject(delivery_tag, requeue)

    def basic_ack(self, delivery_tag, multiple=False):
        super().basic_ack(delivery_tag, multiple=multiple)
        logger.info(f'Acknowledged message "{delivery_tag}"')

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
        logger.debug(payload)
        sqs_msg = payload['properties']['delivery_info']['sqs_message']
        if 'headers' not in payload and sqs_msg['Attributes'].get('MessageGroupId', None):
            # Detected Ergo Protocol, so convert it to Proto 1-compatible schema
            payload = self._to_proto1(payload, sqs_msg)
        return payload

    def _on_messages_ready(self, queue, qname, messages):
        if 'Messages' in messages and messages['Messages']:
            callbacks = self.connection._callbacks
            for msg in messages['Messages']:
                try:
                    msg_parsed = self._message_to_python(msg, qname, queue)
                except JSONDecodeError as e:
                    logger.error(f'Received undecodable message', exc_info=e)
                    continue
                callbacks[qname](msg_parsed)
    
    def change_visibility_timeout(self, delivery_tag, new_visibility_timeout):
        try:
            message = self.qos.get(delivery_tag).delivery_info
            sqs_message = message['sqs_message']
        except KeyError as ex:
            logger.warn('Unable to get message info', exc_info=ex)
            return
        queue = None
        if 'routing_key' in message:
            queue = self.canonical_queue_name(message['routing_key'])
        try:
            logger.info(message['sqs_queue'] + '\n' + sqs_message['ReceiptHandle'] + f'\n{new_visibility_timeout}')
            client = self.sqs(queue)
            client.change_message_visibility(
                QueueUrl=message['sqs_queue'],
                ReceiptHandle=sqs_message['ReceiptHandle'],
                VisibilityTimeout=int(new_visibility_timeout)
            )
        except ClientError as ex:
            logger.warn('Errored changing visibility timeout', exc_info=ex)
            logger.warn(ex.response)

    # TODO: Better to add the FIFO queue support in Kombu itself (reminder: Raise a PR)
    def _get_bulk(self, queue,
                  max_if_unlimited=SQS.SQS_MAX_MESSAGES, callback=None):
        """Try to retrieve multiple messages off ``queue``.

        Where :meth:`_get` returns a single Payload object, this method
        returns a list of Payload objects.  The number of objects returned
        is determined by the total number of messages available in the queue
        and the number of messages the QoS object allows (based on the
        prefetch_count).

        Note:
            Ignores QoS limits so caller is responsible for checking
            that we are allowed to consume at least one message from the
            queue.  get_bulk will then ask QoS for an estimate of
            the number of extra messages that we can consume.

        Arguments:
            queue (str): The queue name to pull from.

        Returns:
            List[Message]
        """
        # drain_events calls `can_consume` first, consuming
        # a token, so we know that we are allowed to consume at least
        # one message.

        # Note: ignoring max_messages for SQS with boto3
        max_count = self._get_message_estimate()
        if max_count:
            q_url = self._new_queue(queue)
            resp = self.sqs(queue=queue).receive_message(
                QueueUrl=q_url, MaxNumberOfMessages=max_count,
                WaitTimeSeconds=self.wait_time_seconds,
                VisibilityTimeout=self.visibility_timeout,
                AttributeNames=['MessageGroupId'])
            if resp.get('Messages'):
                for m in resp['Messages']:
                    m['Body'] = AsyncMessage(body=m['Body']).decode()
                for msg in self._messages_to_python(resp['Messages'], queue):
                    self.connection._deliver(msg, queue)
                return
        raise Empty()

    # TODO: Better to add the FIFO queue support in Kombu itself (reminder: Raise a PR)
    def _get_from_sqs(self, queue,
                      count=1, connection=None, callback=None):
        """Retrieve and handle messages from SQS.

        Uses long polling and returns :class:`~vine.promises.promise`.
        """
        connection = connection if connection is not None else queue.connection
        if self.predefined_queues:
            if queue not in self._queue_cache:
                raise SQS.UndefinedQueueException((
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


class SQSTransport(SQS.Transport):
    Channel = ErgoChannel
