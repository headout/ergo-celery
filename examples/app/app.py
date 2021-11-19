from celery import Celery
from celery import Task
from kombu.transport import TRANSPORT_ALIASES

from ergo_celery.request.ping_timer import SQSPingTimerStep
from ergo_celery.result.drain_timer import ResultTimerStep

from . import config

TRANSPORT_ALIASES['ergosqs'] = 'ergo_celery.request.transport.SQSTransport'

class CeleryTask(Task):
    """
    Base Task class to use with all selenium-related celery tasks
    """

    Request = 'ergo_celery.request.request:SQSRequest'

app = Celery("app", task_cls=CeleryTask)
app.config_from_object(config)
app.steps['worker'].add(ResultTimerStep)
app.steps['worker'].add(SQSPingTimerStep)
