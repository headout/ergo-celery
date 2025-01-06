import time
from celery import Celery
from celery.signals import task_prerun, task_postrun
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

class CeleryMonitor:
    def __init__(self, app: Celery, pushgateway_url: str):
        self.app = app
        self.pushgateway_url = pushgateway_url
        self.registry = CollectorRegistry()
        self.task_duration = Gauge(
            'celery_task_duration_seconds',
            'Duration of Celery tasks in seconds',
            ['task_name'],
            registry=self.registry
        )
        self.task_status = Gauge(
            'celery_task_status',
            'Status of Celery tasks: 0=received, 1=started, 2=succeeded, 3=failed',
            ['task_name'],
            registry=self.registry
        )

        task_prerun.connect(self.task_prerun_handler, sender=self.app)
        task_postrun.connect(self.task_postrun_handler, sender=self.app)

    def task_prerun_handler(self, sender=None, task_id=None, task=None, *args, **kwargs):
        task.__start_time__ = time.time()
        self.task_status.labels(task_name=task.name).set(1)  # Task started
        self.push_metrics()

    def task_postrun_handler(self, sender=None, task_id=None, task=None, retval=None, state=None, *args, **kwargs):
        start_time = getattr(task, '__start_time__', None)
        if start_time:
            duration = time.time() - start_time
            self.task_duration.labels(task_name=task.name).set(duration)
        if state == 'SUCCESS':
            self.task_status.labels(task_name=task.name).set(2)  # Task succeeded
        elif state == 'FAILURE':
            self.task_status.labels(task_name=task.name).set(3)  # Task failed
        self.push_metrics()

    def push_metrics(self):
        push_to_gateway(self.pushgateway_url, job='celery', registry=self.registry)

    def start_monitoring(self):
        state = self.app.events.State()

        def announce_task_received(event):
            state.event(event)
            task = state.tasks.get(event['uuid'])
            if task:
                print(f'TASK RECEIVED: {task.name}[{task.uuid}]')
                self.task_status.labels(task_name=task.name).set(0)  # Task received
                self.push_metrics()

        def announce_task_succeeded(event):
            state.event(event)
            task = state.tasks.get(event['uuid'])
            if task:
                print(f'TASK SUCCEEDED: {task.name}[{task.uuid}]')
                self.task_status.labels(task_name=task.name).set(2)  # Task succeeded
                self.push_metrics()

        def announce_task_failed(event):
            state.event(event)
            task = state.tasks.get(event['uuid'])
            if task:
                print(f'TASK FAILED: {task.name}[{task.uuid}]')
                self.task_status.labels(task_name=task.name).set(3)  # Task failed
                self.push_metrics()

        with self.app.connection() as connection:
            recv = self.app.events.Receiver(connection, handlers={
                'task-received': announce_task_received,
                'task-succeeded': announce_task_succeeded,
                'task-failed': announce_task_failed,
                '*': state.event,
            })
            recv.capture(limit=None, timeout=None, wakeup=True)

