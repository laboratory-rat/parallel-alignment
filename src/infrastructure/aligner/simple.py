from typing import Tuple, List

import numpy as np

from src.domain.sequence_data import SequenceData
from src.infrastructure.aligner.base import AlignerParams, Aligner


class AlignerSimple(Aligner):
    params: AlignerParams
    familiar_pairs = [('A', 'G'), ('C', 'T'), ('C', 'U')]

    def __init__(self, params: AlignerParams = AlignerParams()):
        self.params = params

    def clone(self) -> 'Aligner':
        return AlignerSimple(self.params)

    def get_pair_score(self, a: str, b: str) -> float:
        if a == b:
            return self.params.match_score

        if (a, b) in self.familiar_pairs or (b, a) in self.familiar_pairs:
            return self.params.familiar_replace_panalty

        return self.params.non_familiar_replace_penalty

    def needleman_wunsch(self, seq1, seq2) -> Tuple[str, str, float]:
        n, m = len(seq1), len(seq2)
        score = np.zeros((n + 1, m + 1))
        gap_penalty = self.params.gap_penalty

        for i in range(n + 1):
            score[i][0] = gap_penalty * i
        for j in range(m + 1):
            score[0][j] = gap_penalty * j

        for i in range(1, n + 1):
            for j in range(1, m + 1):
                match = score[i - 1][j - 1] + self.get_pair_score(seq1[i - 1], seq2[j - 1])
                delete = score[i - 1][j] + gap_penalty
                insert = score[i][j - 1] + gap_penalty
                score[i][j] = max(match, delete, insert)

        # Traceback
        align1, align2 = '', ''
        i, j = n, m
        while i > 0 and j > 0:
            score_current = score[i][j]
            score_diagonal = score[i - 1][j - 1]
            score_up = score[i][j - 1]
            score_left = score[i - 1][j]

            if score_current == score_diagonal + self.get_pair_score(seq1[i - 1], seq2[j - 1]):
                align1 += seq1[i - 1]
                align2 += seq2[j - 1]
                i -= 1
                j -= 1
            elif score_current == score_left + gap_penalty:
                align1 += seq1[i - 1]
                align2 += '-'
                i -= 1
            elif score_current == score_up + gap_penalty:
                align1 += '-'
                align2 += seq2[j - 1]
                j -= 1

        while i > 0:
            align1 += seq1[i - 1]
            align2 += '-'
            i -= 1
        while j > 0:
            align1 += '-'
            align2 += seq2[j - 1]
            j -= 1

        return align1[::-1], align2[::-1], float(score[n][m])

    def align_to_existing_alignment(self, msa, new_seq):
        consensus = ""
        for i in range(len(msa[0])):
            column = [seq[i] for seq in msa if seq[i] != '-']
            consensus += max(set(column), key=column.count) if column else '-'

        n, m = len(consensus), len(new_seq)
        score = np.zeros((n + 1, m + 1))
        score[:, 0] = np.arange(n + 1) * self.params.gap_penalty
        score[0, :] = np.arange(m + 1) * self.params.gap_penalty

        for i in range(1, n + 1):
            for j in range(1, m + 1):
                match = score[i - 1, j - 1] + (self.get_pair_score(consensus[i - 1], new_seq[j - 1]))
                delete = score[i - 1, j] + self.params.gap_penalty
                insert = score[i, j - 1] + self.params.gap_penalty
                score[i, j] = max(match, delete, insert)

        # Traceback to find the optimal alignment
        align_consensus, align_new_seq = '', ''
        i, j = n, m
        while i > 0 and j > 0:
            if score[i, j] == score[i - 1, j - 1] + self.get_pair_score(consensus[i - 1], new_seq[j - 1]):
                align_consensus += consensus[i - 1]
                align_new_seq += new_seq[j - 1]
                i -= 1
                j -= 1
            elif score[i, j] == score[i, j - 1] + self.params.gap_penalty:
                align_consensus += '-'
                align_new_seq += new_seq[j - 1]
                j -= 1
            else:
                align_consensus += consensus[i - 1]
                align_new_seq += '-'
                i -= 1

        while i > 0:
            align_consensus += consensus[i - 1]
            align_new_seq += '-'
            i -= 1
        while j > 0:
            align_consensus += '-'
            align_new_seq += new_seq[j - 1]
            j -= 1

        align_consensus, align_new_seq = align_consensus[::-1], align_new_seq[::-1]

        updated_msa = []
        for seq in msa:
            updated_seq = ''
            seq_index, new_seq_index = 0, 0
            for c in align_consensus:
                if c != '-':
                    updated_seq += seq[seq_index]
                    seq_index += 1
                else:
                    updated_seq += '-'
            updated_msa.append(updated_seq)

        updated_msa.append(align_new_seq)

        return updated_msa

    def process_batch(self, batch: List[SequenceData]) -> List[SequenceData]:
        if len(batch) < 2:
            return batch

        (seq1, seq2, _) = self.needleman_wunsch(batch[0].sequence, batch[1].sequence)
        sliced_batch = batch[2:]
        batch = [batch[0], batch[1]] + sliced_batch
        existing_alignment = [seq1, seq2]

        for sequence in batch[2:]:
            new_values = self.align_to_existing_alignment(existing_alignment, sequence.sequence)
            for index in range(len(new_values) - 1):
                batch[index].sequence = new_values[index]
            sequence.sequence = new_values[-1]
            existing_alignment.append(new_values[-1])

        return batch
