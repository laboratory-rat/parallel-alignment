from typing import List, Tuple


class Metadata:
    sequences: List[Tuple[float, float]] = None

    def __init__(self):
        self.sequences = []

    def add(self, timestart: float, timeend: float):
        self.sequences.append((timestart, timeend))
