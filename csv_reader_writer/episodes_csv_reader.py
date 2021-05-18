import ast
import logging
from typing import List

import numpy as np
import pandas as pd

from model.podcast.episode import Episode
from util.dates import ymd_to_date


class EpisodesCsvReader:
    def read_to_episodes(self, path: str) -> List[Episode]:
        def _nan_to_none(v: any) -> any:
            return v if not pd.isnull(v) else None

        episodes = []
        frame = pd.read_csv(path,
                            parse_dates=['episode_date'],
                            date_parser=np.vectorize(ymd_to_date),
                            converters={
                                'cids': ast.literal_eval,
                                'category_names': ast.literal_eval
                            })
        logging.info(f"Read CSV file from: {path}")
        for i, row in frame.iterrows():
            episode = Episode(**{k: _nan_to_none(v) for k, v in row.to_dict().items()})
            episodes.append(episode)
        return episodes
