from contextlib import contextmanager
from typing import Dict, List

from kombu.connection import Connection
from kombu.transport.redis import Channel as RedisChannel
from kombu.utils.json import dumps, loads


class RedisResultBuffer(object):
    def __init__(self, name, celery_app, max_size) -> None:
        self.name = name
        self.celery_app = celery_app
        self.max_size = max_size

    @contextmanager
    def _client(self):
        conn: Connection = self.celery_app.connection_for_write()
        channel: RedisChannel = conn.channel()
        if not isinstance(channel, RedisChannel):
            raise RuntimeError(f'Using Redis result buffer but write broker does not use Redis, instead {type(channel)}')
        with channel.conn_or_acquire() as client:
            yield client
        channel.close()
        conn.close()

    def __len__(self):
        with self._client() as client:
            return client.llen(self.name)

    def put(self, msg):
        with self._client() as client:
            client.rpush(self.name, dumps(msg))

    def clear(self) -> List[Dict]:
        # with self._new_buffer() as obj:
        #     obj.buffer.
        with self._client() as client:
            arr = client.lrange(self.name, 0, self.max_size)
            msgs = [
                loads(res.decode())
                for res in arr
            ]
            client.ltrim(self.name, self.max_size + 1, -1)
            return msgs
