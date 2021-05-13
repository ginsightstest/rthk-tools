import logging
from typing import List

import pandas as pd

from model.podcast.episode import Episode


class EpisodesCsvReader:
    def read_to_episodes(self, path: str) -> List[Episode]:
        episodes = []
        frame = pd.read_csv(path)
        logging.info(f"Read CSV file from: {path}")
        for i, row in frame.iterrows():
            episode = Episode(**row.to_dict())
            episodes.append(episode)
        return episodes
