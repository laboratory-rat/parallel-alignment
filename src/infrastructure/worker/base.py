from abc import ABC, abstractmethod
from typing import Callable, TypeVar, Generic

BatchType = TypeVar('BatchType')


class Worker(ABC, Generic[BatchType]):
    workers: int
    io: bool
    fn: Callable[[BatchType], BatchType] = None

    def __init__(self, workers: int, io: bool = False):
        self.workers = workers
        self.io = io

    def setup(self, fn: Callable[[BatchType], BatchType]):
        self.fn = fn

    @abstractmethod
    def run(self, batches: BatchType) -> BatchType:
        pass

    def close(self):
        pass

    def console(self, message: object):
        if self.io:
            print(message)

    def input(self, message: object):
        if self.io:
            return input(message)

        return None
