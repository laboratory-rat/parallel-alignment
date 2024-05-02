from abc import ABC, abstractmethod
from typing import List, Tuple

from pydantic import BaseModel

from src.domain.sequence_data import SequenceData


class AlignerParams(BaseModel):
    gap_penalty: float = -1
    familiar_replace_panalty: float = -.5
    non_familiar_replace_penalty: float = -1.5
    match_score: float = 1


class Aligner(ABC):
    @abstractmethod
    def clone(self) -> 'Aligner':
        pass

    @abstractmethod
    def get_pair_score(self, a: str, b: str) -> float:
        pass

    @abstractmethod
    def needleman_wunsch(self, seq1, seq2) -> Tuple[str, str, float]:
        pass

    @abstractmethod
    def align_to_existing_alignment(self, msa, new_seq):
        pass

    @abstractmethod
    def process_batch(self, batch: List[SequenceData]) -> List[SequenceData]:
        pass
