from abc import ABC, abstractmethod
from multiprocessing import Queue, Process
from multiprocessing.pool import Pool
from typing import List, Callable

from src.domain.sequence_data import SequenceData


class MPWorker(ABC):
    fn: Callable = None

    def setup(self, fn: Callable):
        self.fn = fn

    @abstractmethod
    def run(self, batches: List[List[SequenceData]]) -> List[List[SequenceData]]:
        pass

    @abstractmethod
    def close(self):
        pass


class MPWorkerSync(MPWorker):
    fn: Callable = None

    def __init__(self):
        pass

    def run(self, batches: List[List[SequenceData]]) -> List[List[SequenceData]]:
        updated = []
        for batch in batches:
            updated.append(self.fn(batch))

        return updated

    def close(self):
        pass


class MPWorkerPool(MPWorker):
    pool: Pool = None

    def setup(self, fn: Callable):
        super().setup(fn)
        self.pool = Pool()

    def run(self, batches: List[List[SequenceData]]) -> List[List[SequenceData]]:
        return self.pool.map(self.fn, batches)

    def close(self):
        if self.pool:
            self.pool.close()
            self.pool.join()


class MPWorkerQueue(MPWorker):
    queue: Queue = None
    consumer_process = None
    stopped = False
    results = []

    def setup(self, fn: Callable):
        super().setup(fn)
        self.queue = Queue()
        self.consumer_process = Process(target=self._consumer, args=(self.queue, fn))
        self.stopped = False

    def run(self, batches: List[List[SequenceData]]) -> List[List[SequenceData]]:
        self.results = []
        for batch in batches:
            self.queue.put(batch)

        self.queue.get_nowait()
        return self.results

    def close(self):
        self.stopped = True
        self.consumer_process.join()
        self.queue.close()

    def _consumer(self, queue: Queue, fn: Callable) -> None:
        while not self.stopped:
            item = queue.get()
            if item is None:
                break

            self.results.append(fn(item))
