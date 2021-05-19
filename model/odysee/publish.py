from typing import List, NamedTuple, Optional


class OdyseeChannelCreateApiRequest(NamedTuple):
    name: str
    title: str
    description: str
    bid: str
    account_id: Optional[str] = None
    allow_duplicate_name: bool = False
    blocking: bool = True
    claim_address: Optional[str] = None
    cover_url: Optional[str] = None
    email: Optional[str] = None
    featured: List[str] = []
    funding_account_ids: List[str] = []
    languages: List[str] = []
    locations: List[str] = []
    preview: bool = False
    tags: List[str] = []
    thumbnail_url: Optional[str] = None
    wallet_id: Optional[str] = None
    website_url: Optional[str] = None


class OdyseePublishApiRequest(NamedTuple):
    name: str
    title: str
    description: str
    file_path: str
    channel_id: str
    bid: str
    account_id: Optional[str] = None
    author: Optional[str] = None
    blocking: bool = True
    channel_account_id: List[str] = []
    channel_name: Optional[str] = None
    claim_address: Optional[str] = None
    fee_address: Optional[str] = None
    fee_amount: Optional[float] = None
    fee_currency: Optional[str] = None
    file_hash: Optional[str] = None
    file_name: Optional[str] = None
    funding_account_ids: List[str] = []
    languages: List[str] = []
    license: Optional[str] = None
    license_url: Optional[str] = None
    locations: List[str] = []
    optimize_file: bool = False
    preview: bool = False
    release_time: Optional[int] = None
    sd_hash: Optional[str] = None
    tags: List[str] = []
    thumbnail_url: Optional[str] = None
    validate_file: bool = False
    wallet_id: Optional[str] = None
