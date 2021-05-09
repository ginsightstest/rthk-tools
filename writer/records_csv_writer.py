import logging
from typing import Collection

import pandas as pd

from model.record import Record


class RecordsCsvWriter:
    def __init__(self, records: Collection[Record]):
        self._records = records

    def write_to_csv(self, path: str):
        frame = pd.DataFrame.from_records([r._asdict() for r in self._records]) \
            .astype({'episode': 'Int64'}) \
            .sort_values(by=["programme", "episode", "dt", "title"]) \
            .rename(columns={'dt': 'date'})
        frame.to_csv(path, index=False)
        logging.info(f"Wrote CSV file to: {path}")
