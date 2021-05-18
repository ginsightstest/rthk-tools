import ast
import logging
import math
from typing import List

import numpy as np
import pandas as pd

from model.podcast.episode import Episode
from util.dates import ymd_to_date


class EpisodesCsvReader:
    def read_to_episodes(self, path: str) -> List[Episode]:
        def _nan_to_none(v: any) -> any:
            if isinstance(v, float) and math.isnan(v):
                return None
            return v

        def _parse_list(s: str) -> List[str]:
            return ast.literal_eval(s) if s else []

        episodes = []
        frame = pd.read_csv(path,
                            parse_dates=['episode_date'],
                            date_parser=np.vectorize(ymd_to_date),
                            converters={
                                'cids': _parse_list,
                                'category_names': _parse_list
                            })
        logging.info(f"Read CSV file from: {path}")
        for i, row in frame.iterrows():
            episode = Episode(**{k: _nan_to_none(v) for k, v in row.to_dict().items()})
            episodes.append(episode)
        return episodes
