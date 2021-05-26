import logging
import math

import numpy as np
import pandas as pd

from model.backup_table import BackupTable, BackupTableRow
from util.dates import ymd_to_date


class BackupTableReader:
    def read_to_backup_table(self, path: str) -> BackupTable:
        def _nan_to_none(v: any) -> any:
            if isinstance(v, float) and math.isnan(v):
                return None
            return v

        backup_table = []
        frame = pd.read_csv(path,
                            parse_dates=['date'],
                            date_parser=np.vectorize(ymd_to_date)) \
            .rename(columns={'date': 'dt'})
        logging.info(f"Read CSV file from: {path}")

        for i, row in frame.iterrows():
            backup_table_row = BackupTableRow(**{k: _nan_to_none(v) for k, v in row.to_dict().items()})
            backup_table.append(backup_table_row)
        return backup_table
