from datetime import datetime
from typing import List, Tuple


class Metadata:
    sequences: List[Tuple[str, str]] = []

    def add(self, timestart: datetime, timeend: datetime):
        formatted_date_start = timestart.strftime("%Y-%m-%d %H:%M:%S")
        formatted_date_end = timeend.strftime("%Y-%m-%d %H:%M:%S")

        self.sequences.append((formatted_date_start, formatted_date_end))
