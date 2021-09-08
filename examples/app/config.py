broker_url = 'sqs://<AWSAccessKeyId>:<AWSSecretAccessKey>@localhost:9324/'
broker_transport = 'ergo_celery.transport:SQSTransport'
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Asia/Kolkata'

broker_transport_options = {
    'predefined_queues': {
        'fifo_req_calipso': {
            'url': 'http://localhost:9324/queue/fifo_req_calipso',
        },
        'fifo_req_aries': {
            'url': 'http://localhost:9324/queue/fifo_req_aries',
        },
        'fifo_res': {
            'url': 'http://localhost:9324/queue/fifo_res'
        }
    }
}

task_routes = {
    'app.tasks.calipso*': {'queue': 'fifo_req_calipso'}
}

# worker_consumer = 'ergo_celery.ergo_consumer:ErgoConsumer'
result_backend = 'ergo_celery.sqs_backend:SQSBackend://http://localhost:9324/queue/fifo_res'
