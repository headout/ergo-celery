from celery import Celery
from . import config

app = Celery("app")
app.config_from_object(config)