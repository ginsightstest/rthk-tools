from typing import List, NamedTuple


class Programme(NamedTuple):
    pid: int  # programme id
    title: str
    format: str  # 'video' / 'audio'


class ProgrammeInfo(NamedTuple):
    pid: int
    title: str
    description: str
    language: str  # '中文' / '英文'
    category_names: List[str]
    rss_url: str
