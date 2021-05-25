from datetime import date
from typing import List, NamedTuple


class BackupTableRow(NamedTuple):
    programme: str
    dt: date
    title: str
    content_description: str
    youtube_link: str
    backup_link: str


BackupTable = List[BackupTableRow]
