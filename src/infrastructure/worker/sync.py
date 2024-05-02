from typing import Generic, Callable

from src.infrastructure.worker.base import Worker, BatchType


class WorkerSync(Worker, Generic[BatchType]):
    fn: Callable = None

    def run(self, batches: BatchType) -> BatchType:
        updated = []
        for batch in batches:
            updated.append(self.fn(batch))

        return updated
