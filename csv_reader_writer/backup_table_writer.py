import logging

import pandas as pd

from model.backup_table import BackupTable


class BackupTableWriter:
    def __init__(self, backup_table: BackupTable):
        self._backup_table = backup_table

    def write_to_csv(self, path: str):
        frame = pd.DataFrame.from_records([row._asdict() for row in self._backup_table]) \
            .sort_values(by=["programme", "dt"]) \
            .rename(columns={'dt': 'date'})
        frame.to_csv(path, index=False)
        logging.info(f"Wrote CSV file to: {path}")
