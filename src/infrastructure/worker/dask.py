import random
import webbrowser
from typing import Generic, Callable

from distributed import LocalCluster, Client

from src.infrastructure.worker.base import Worker, BatchType


class WorkerDask(Worker, Generic[BatchType]):
    cluster: LocalCluster = None
    client: Client = None
    port: int = random.randint(7000, 9999)

    @property
    def local_address(self):
        return f'http://localhost:{self.port}/status'

    def setup(self, fn: Callable[[BatchType], BatchType]):
        super().setup(fn)
        self.cluster = LocalCluster(dashboard_address=f':{self.port}', n_workers=self.workers)
        self.client = self.cluster.get_client()

        if self.io:
            webbrowser.open(self.local_address)
        self.console(f'View the dashboard at {self.local_address}')

    def run(self, batches: BatchType) -> BatchType:
        features = [self.client.submit(self.fn, batch) for batch in batches]
        return self.client.gather(features)

    def close(self):
        self.input('Press Enter to close the dashboard')
        super().close()
        self.client.close()
        self.cluster.close()
