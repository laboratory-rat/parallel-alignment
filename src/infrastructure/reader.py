from Bio import SeqIO
from typing import Literal, List

from src.domain.sequence_data import SequenceData


class Reader:
    filename: str
    filetype: Literal['fasta']

    def __init__(self, filename):
        self.filename = filename
        self.filetype = 'fasta'

    def read(self) -> List[SequenceData]:
        sequences = list(SeqIO.parse(self.filename, self.filetype))
        return [SequenceData.from_fasta(sequence) for sequence in sequences]
