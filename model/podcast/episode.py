from datetime import date
from typing import List, NamedTuple


class Episode(NamedTuple):
    pid: int  # programme id
    eid: int  # episode id
    programme_title: str = None
    episode_title: str = None
    episode_date: date = None
    duration_seconds: int = None
    og_title: str = None
    og_description: str = None
    cids: List[int] = []  # category id
    category_names: List[str] = []
    file_url: str = None
    m3u8_url: str = None  # 250000:  256x144 , 400000:  432x240 , 700000:  640x360 , 1000000: 848x480 , 2000000: 1280x720
    rss_url: str = None
    language: str = None  # '中文' / '英文'
    format: str = None  # 'video' / 'audio'
