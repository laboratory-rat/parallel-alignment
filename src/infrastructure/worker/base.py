from abc import ABC, abstractmethod
from typing import Callable, TypeVar, Generic

BatchType = TypeVar('BatchType')


class Worker(ABC, Generic[BatchType]):
    workers: int
    fn: Callable[[BatchType], BatchType] = None

    def __init__(self, workers: int):
        self.workers = workers

    def setup(self, fn: Callable[[BatchType], BatchType]):
        self.fn = fn

    @abstractmethod
    def run(self, batches: BatchType) -> BatchType:
        pass

    def close(self):
        pass
