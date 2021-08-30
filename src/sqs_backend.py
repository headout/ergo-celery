from celery.backends.asynchronous import AsyncBackendMixin, BaseResultConsumer
from celery.backends.base import Backend
from celery.utils.log import get_logger
from kombu.transport.SQS import SQS_MAX_MESSAGES

logger = get_logger(__name__)

STATUS_MAPPING = {
    'SUCCESS': 200,
    'FAILURE': 400
}

class SQSBackend(Backend):
    def __init__(self, *args, max_buffer_size=SQS_MAX_MESSAGES, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_buffer_size = max_buffer_size
        self._pending_results = {}
        self._connection = self.connection_for_write()

    def connection_for_write(self):
        return self.ensure_connected(
            self.app.connection_for_write(self.as_uri()))

    def ensure_connected(self, conn):
        return conn.ensure_connection()

    def _drain_results(self):
        msgs = self._pending_results.values()
        try:
            success, failures = self._connection.default_channel.put_bulk(self.as_name(), msgs)
            if failures:
                raise RuntimeError(f'{failures}')
        except Exception as e:
            logger.error('Failed pushing results', exc_info=e)
        else:
            logger.info(f'Successfully pushed {len(success)} results!')
            self._pending_results.clear()

    def _add_pending_result(self, task_id, result):
        if task_id not in self._pending_results:
            self._pending_results[task_id] = result
            if len(self._pending_results) == self.max_buffer_size:
                self._drain_results()

    def _get_result_meta(self, job_id, result, state, traceback, request):
        if traceback:
            result = None
        meta = {
            'taskId': request.task,
            'jobId': job_id,
            'data': result or {},
            'metadata': {
                'status': STATUS_MAPPING[state],
                'error': traceback or None
            }
        }
        return meta

    def _store_result(self, task_id, result, state,
                      traceback=None, request=None, **kwargs):
        meta = self._get_result_meta(task_id, result, state, traceback, request)
        self._add_pending_result(task_id, meta)

    def as_uri(self, include_password=True):
        return self.url.split('://', 1)[1]

    def as_name(self):
        url = self.as_uri()
        return url.split('/')[-1]

    @property
    def is_async(self):
        return True
