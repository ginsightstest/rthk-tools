import logging
from typing import Collection

import pandas as pd

from model.podcast.episode import Episode


class EpisodesCsvWriter:
    def __init__(self, episodes: Collection[Episode]):
        self._episodes = episodes

    def write_to_csv(self, path: str):
        frame = pd.DataFrame.from_records([e._asdict() for e in self._episodes]) \
            .sort_values(by=["pid", "eid"])
        frame.to_csv(path, index=False)
        logging.info(f"Wrote CSV file to: {path}")
