import subprocess
import time
from typing import Generic
from celery import Celery
from kombu.serialization import register
from pydantic import BaseModel
import json

from src.infrastructure.worker.base import BatchType, Worker
import src.domain as domain
from threading import Thread


class PydanticSerializer(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, BaseModel):
            return obj.model_dump() | {'__type__': type(obj).__name__}
        else:
            return json.JSONEncoder.default(self, obj)


def pydantic_decoder(obj):
    if '__type__' in obj:
        if obj['__type__'] in dir(domain):
            cls = getattr(domain, obj['__type__'])
            return cls.parse_obj(obj)
    return obj


def pydantic_dumps(obj):
    return json.dumps(obj, cls=PydanticSerializer)


def pydantic_loads(obj):
    return json.loads(obj, object_hook=pydantic_decoder)


register(
    "pydantic",
    pydantic_dumps,
    pydantic_loads,
    content_type="application/x-pydantic",
    content_encoding="utf-8",
)


def check_redis_ready():
    import redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    while True:
        try:
            if r.ping():
                print("Redis is ready.")
                break
        except redis.ConnectionError:
            print("Waiting for Redis...")
            time.sleep(1)


class WorkerCelery(Worker, Generic[BatchType]):
    redis_url: str = 'redis://localhost:6379/0'
    app: Celery = None
    worker_thread = None

    def setup(self, fn):
        process = subprocess.Popen('docker run -d -p 6379:6379 redis')
        process.wait()
        check_redis_ready()

        self.app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
        self.app.conf.update(
            task_serializer="pydantic",
            result_serializer="pydantic",
            event_serializer="pydantic",
            accept_content=["application/json", "application/x-pydantic"],
            timezone='Europe/London',
            enable_utc=True,
        )

        self.app.task(name='process_task', bind=True)(fn)
        self.worker_thread = Thread(target=lambda: self.app.worker_main(['worker', '--loglevel=info']))
        self.worker_thread.start()

    def run(self, batches):
        results = []
        for batch in batches:
            result = self.app.send_task('process_task', args=[batch])
            results.append(result.get())

        return results

    def close(self):
        super().close()
        self.app.control.shutdown()
        self.worker_thread.join(1)
        subprocess.Popen('docker stop $(docker ps -a -q)')
