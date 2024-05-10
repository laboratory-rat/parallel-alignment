from multiprocessing import Pool
from typing import Generic, Callable

from src.infrastructure.worker.base import Worker, BatchType


class WorkerPool(Worker, Generic[BatchType]):
    pool: Pool

    def setup(self, fn: Callable[[BatchType], BatchType]):
        super().setup(fn)
        self.pool = Pool(self.workers)

    def run(self, batches: BatchType) -> BatchType:
        return self.pool.map(self.fn, batches)

    def close(self):
        self.pool.close()
