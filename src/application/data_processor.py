import time
from typing import List

from pydantic import BaseModel

from src.domain.helpers import helper_split_to_batches_generator
from src.domain.sequence_data import SequenceData
from src.infrastructure.aligner.base import Aligner
from src.infrastructure.logger import AppLogger
from src.infrastructure.reader import Reader
from src.infrastructure.worker.base import Worker


class ProcessorProps(BaseModel):
    limit: int | None = None


class ProcessorMetadata(BaseModel):
    total_time_ms: int
    processing_time_ms: int


class ProcessorResult(BaseModel):
    value: List[SequenceData]
    metadata: ProcessorMetadata


class DataProcessor:
    props: ProcessorProps
    file_reader: Reader
    aligner: Aligner
    logger: AppLogger = AppLogger()
    worker: Worker[List[SequenceData]]
    first_batch_size = 10

    def __init__(self, props: ProcessorProps, mp_worker, file_reader: Reader, aligner: Aligner):
        self.props = props
        self.file_reader = file_reader
        self.aligner = aligner
        self.worker = mp_worker

    # def process(self) -> List[SequenceData]:
    def process(self) -> ProcessorResult:
        start_time_full = time.time()
        self.worker.setup(self.aligner.process_batch)
        start_time_processing = time.time()
        sequences_data_list = self.file_reader.read()

        def _create_metadata(now: float = None) -> ProcessorMetadata:
            now = now if now else time.time()
            return ProcessorMetadata(
                processing_time_ms=int((now - start_time_processing) * 1000),
                total_time_ms=int((now - start_time_full) * 1000),
            )

        if self.props.limit and len(sequences_data_list) > self.props.limit:
            sequences_data_list = sequences_data_list[:self.props.limit]

        if not len(sequences_data_list):
            raise ValueError('No sequences found in the file')

        batches = list(helper_split_to_batches_generator(sequences_data_list, self.first_batch_size))

        if len(batches) == 0:
            return ProcessorResult(value=[], metadata=_create_metadata())

        if len(batches) == 1:
            result = self.aligner.process_batch(batches[0])
            return ProcessorResult(value=result, metadata=_create_metadata())

        if len(batches[-1]) < 3:
            batches = batches[:-2] + [batches[-2] + batches[-1]]

        while len(batches) > 1:
            updated_batches = self.worker.run(batches)
            if len(updated_batches) % 2 != 0:
                updated_batches[-2] = updated_batches[-1] + updated_batches[-2]
                updated_batches = updated_batches[:-1]

            batches = [updated_batches[i] + updated_batches[i + 1] for i in range(0, len(updated_batches), 2)]

        timer_stop = time.time()
        self.worker.close()
        return ProcessorResult(value=batches[0], metadata=_create_metadata(timer_stop))
