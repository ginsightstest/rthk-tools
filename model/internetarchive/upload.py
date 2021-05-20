from typing import NamedTuple, Optional


class InternetArchiveUploadApiRequest(NamedTuple):
    identifier: str
    title: str
    description: str
    mediatype: str
    file_path: str
    collection: Optional[str] = None
    creator: Optional[str] = None
    date: Optional[str] = None
