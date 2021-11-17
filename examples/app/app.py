from celery import Celery
from kombu.transport import TRANSPORT_ALIASES

from ergo_celery.result_timer import ResultTimerStep

from . import config

TRANSPORT_ALIASES['ergosqs'] = 'ergo_celery.transport.SQSTransport'

app = Celery("app")
app.config_from_object(config)
app.steps['worker'].add(ResultTimerStep)
