from celery import Celery

app = Celery(
    'stock_updater',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/1'
)

app.conf.update(
    task_ignore_result = True
    # result_expires=180,
    # task_serializer='json',
    # accept_content=['json'],
    # result_serializer='json',
    # timezone='UTC',
    # enable_utc=True
)
