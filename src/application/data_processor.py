import sys
from multiprocessing import Pool
from typing import List

from pydantic import BaseModel

from src.domain.sequence_data import SequenceData
from src.infrastructure.aligner import Aligner
from src.infrastructure.logger import AppLogger
from src.infrastructure.reader import Reader


class ProcessorProps(BaseModel):
    multiprocessing: bool = False
    num_processors: int = 1
    limit: int | None = None


class DataProcessor:
    props: ProcessorProps
    file_reader: Reader
    aligner: Aligner
    first_batch_size = 20
    logger: AppLogger = AppLogger()

    def __init__(self, props: ProcessorProps, file_reader: Reader, aligner: Aligner):
        self.props = props
        self.file_reader = file_reader
        self.aligner = aligner

    def process(self) -> List[SequenceData]:
        sequences_data_list = self.file_reader.read()
        if self.props.limit and len(sequences_data_list) > self.props.limit:
            sequences_data_list = sequences_data_list[:self.props.limit]

        if not len(sequences_data_list):
            raise ValueError('No sequences found in the file')

        parallel_number = max(self.props.num_processors if self.props.multiprocessing else 1, 1)
        batches = self._to_batch(sequences_data_list, self.first_batch_size)

        if len(batches) == 0:
            return []

        if len(batches[-1]) < 3:
            batches = batches[:-2] + [batches[-2] + batches[-1]]

        if len(batches) == 1:
            return self._process_batch(batches[0])

        initial_batches_count = len(batches)
        total_steps = 0
        while initial_batches_count > 1:
            total_steps += initial_batches_count // 2
            initial_batches_count = (initial_batches_count + 1) // 2

        current_step = 0

        while len(batches) > 1:
            updated_batches = []
            if len(batches) % 2 != 0:
                first_batch = batches.pop(0)
                second_batch = batches.pop(0)
                batches.append(first_batch + second_batch)

            self.logger.log_info(f'Starting step {current_step + 1}/{total_steps}')
            self.logger.log_info(f'Number of batches: {len(batches)}')
            self.logger.log_info(f'Number of sequences: {sum(len(batch) for batch in batches)}')

            if self.props.multiprocessing and len(batches) > 1:
                pool_batches = []
                while len(batches) > 0:
                    max_index = min(parallel_number, len(batches))
                    pool_batches.append(batches[:max_index])
                    batches = batches[max_index:]

                batches = pool_batches
                for batch in batches:
                    with Pool(len(batch)) as pool:
                        updated_batches_ = pool.map(self._process_batch, batch)
                        updated_batches.extend(updated_batches_)
            else:
                for batch in batches:
                    updated_batches.append(self._process_batch(batch))

            batches = [updated_batches[i] + updated_batches[i + 1] for i in range(0, len(updated_batches), 2)]
            current_step += len(updated_batches) // 2
            progress = current_step / total_steps * 100
            self.logger.log_info(f'Progress: {progress:.2f}%')

        return batches[0]

    def _process_pair(self, seq1, seq2):
        return self.aligner.needleman_wunsch(seq1, seq2)

    def _process_batch(self, batch: List[SequenceData]) -> List[SequenceData]:
        if len(batch) < 2:
            return batch

        base_pair = batch[0], batch[1]
        (seq1, seq2, _) = self._process_pair(base_pair[0].sequence, base_pair[1].sequence)
        batch[0].sequence = seq1
        batch[1].sequence = seq2

        existing_alignment = [seq1, seq2]
        for sequence in batch[2:]:
            new_values = self.aligner.align_to_existing_alignment(existing_alignment, sequence.sequence)
            for index in range(len(new_values) - 1):
                batch[index].sequence = new_values[index]
            sequence.sequence = new_values[-1]
            existing_alignment.append(new_values[-1])

        return batch

    @staticmethod
    def _split_sequences(sequences, num_batches):
        if len(sequences) < num_batches * 2:
            return [sequences]

        return [sequences[i::num_batches] for i in range(num_batches)]

    @staticmethod
    def _to_batch(sequences, max_in_batch):
        return [sequences[i:i + max_in_batch] for i in range(0, len(sequences), max_in_batch)]
