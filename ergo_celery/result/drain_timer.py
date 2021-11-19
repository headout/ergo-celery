import logging

from celery import bootsteps

logger = logging.getLogger(__name__)

class ResultTimerStep(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Timer'}

    def __init__(self, worker, *args, **kwargs):
        self.tref = None
        self._backend = worker.app.backend
        self.timeout = worker.app.conf.get('ergo_result_buffer_timeout_secs', 60)

    def start(self, worker):
        self.tref = worker.timer.call_repeatedly(self.timeout, self._backend.drain_results)

    def stop(self, worker):
        if self.tref:
            self.tref.cancel()
            self.tref = None
