from typing import List

from pydantic import BaseModel

from src.domain.sequence_data import SequenceData
from src.infrastructure.aligner import Aligner
from src.infrastructure.logger import AppLogger
from src.infrastructure.mp_worker import MPWorker
from src.infrastructure.reader import Reader


class ProcessorProps(BaseModel):
    limit: int | None = None


class DataProcessor:
    props: ProcessorProps
    file_reader: Reader
    aligner: Aligner
    first_batch_size = 10
    logger: AppLogger = AppLogger()
    mp_worker: MPWorker
    total_steps: int = 0
    current_step: int = 0

    def __init__(self, props: ProcessorProps, mp_worker, file_reader: Reader, aligner: Aligner):
        self.props = props
        self.file_reader = file_reader
        self.aligner = aligner
        self.mp_worker = mp_worker

    def process(self) -> List[SequenceData]:
        self.mp_worker.setup(self._process_batch)
        sequences_data_list = self.file_reader.read()
        if self.props.limit and len(sequences_data_list) > self.props.limit:
            sequences_data_list = sequences_data_list[:self.props.limit]

        if not len(sequences_data_list):
            raise ValueError('No sequences found in the file')

        parallel_number = 1
        batches = self._to_batch(sequences_data_list, self.first_batch_size)
        self.total_steps = self._calculate_steps(len(batches))

        if len(batches) == 0:
            return []

        if len(batches) == 1:
            return self._process_batch(batches[0])

        if len(batches[-1]) < 3:
            batches = batches[:-2] + [batches[-2] + batches[-1]]

        while len(batches) > 1:
            updated_batches = self.mp_worker.run(batches)
            if len(updated_batches) % 2 != 0:
                updated_batches[-2] = updated_batches[-1] + updated_batches[-2]
                updated_batches = updated_batches[:-1]

            batches = [updated_batches[i] + updated_batches[i + 1] for i in range(0, len(updated_batches), 2)]

        self.mp_worker.close()
        return batches[0]

    @staticmethod
    def _calculate_steps(initial_batches_count):
        total_steps = 0
        while initial_batches_count > 1:
            total_steps += initial_batches_count // 2
            initial_batches_count = (initial_batches_count + 1) // 2
        return total_steps

    def _process_pair(self, seq1, seq2):
        return self.aligner.needleman_wunsch(seq1, seq2)

    def _process_batch(self, batch: List[SequenceData]) -> List[SequenceData]:
        if len(batch) < 2:
            return batch

        (seq1, seq2, _) = self._process_pair(batch[0].sequence, batch[1].sequence)
        sliced_batch = batch[2:]
        batch = [batch[0], batch[1]] + sliced_batch
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
