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

    def process(self) -> List[SequenceData]:
        start_time_full = time.time()
        self.worker.setup(self.aligner.process_batch)
        sequences_data_list = self.file_reader.read()

        start_time_only_process = time.time()
        if self.props.limit and len(sequences_data_list) > self.props.limit:
            sequences_data_list = sequences_data_list[:self.props.limit]

        if not len(sequences_data_list):
            raise ValueError('No sequences found in the file')

        batches = list(helper_split_to_batches_generator(sequences_data_list, self.first_batch_size))

        if len(batches) == 0:
            return []

        if len(batches) == 1:
            result = self.aligner.process_batch(batches[0])
            end_time = time.time()
            print(f'Processing time: {end_time - start_time_full}')
            return result

        if len(batches[-1]) < 3:
            batches = batches[:-2] + [batches[-2] + batches[-1]]

        while len(batches) > 1:
            updated_batches = self.worker.run(batches)
            if len(updated_batches) % 2 != 0:
                updated_batches[-2] = updated_batches[-1] + updated_batches[-2]
                updated_batches = updated_batches[:-1]

            batches = [updated_batches[i] + updated_batches[i + 1] for i in range(0, len(updated_batches), 2)]

        end_time_only_process = time.time()
        print(f'Processing timeonly process : {end_time_only_process - start_time_only_process}')
        self.worker.close()
        end_time_full = time.time()
        print(f'Processing time FULL: {end_time_full - start_time_full}')
        return batches[0]
