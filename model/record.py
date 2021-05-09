from datetime import date
from typing import List, NamedTuple, Optional


class Record(NamedTuple):
    programme: Optional[str]
    episode: Optional[int]  # num episode within programme
    dt: Optional[date]
    title: str
    description: str
    youtube_id: Optional[str]
    youtube_fulltitle: Optional[str]
    youtube_categories: Optional[List[str]]
    youtube_upload_date: Optional[date]
    youtube_tags: Optional[List[str]]
