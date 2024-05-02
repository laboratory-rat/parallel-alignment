from multiprocessing import Pool
from typing import Generic

from src.infrastructure.worker.base import Worker, BatchType


class WorkerPool(Worker, Generic[BatchType]):
    def run(self, batches: BatchType) -> BatchType:
        with Pool(self.workers) as pool:
            return pool.map(self.fn, batches)
