import logging

from celery import bootsteps

logger = logging.getLogger(__name__)

class SQSPingTimerStep(bootsteps.StartStopStep):
    requires = {'celery.worker.components:Timer'}

    def __init__(self, worker, *args, **kwargs):
        self.tref = None
        self._backend = worker.app.backend
        self.timeout = 4

    def start(self, worker):
        self.tref = worker.timer.call_repeatedly(
            self.timeout, self.ping_active_tasks, (worker,))

    def stop(self, worker):
        if self.tref:
            self.tref.cancel()
            self.tref = None

    def ping_active_tasks(self, worker):
        for req in worker.state.active_requests:
            req.ping_message()
