from typing import NamedTuple


class Programme(NamedTuple):
    pid: int  # programme id
    title: str
    format: str  # 'video' / 'audio'
