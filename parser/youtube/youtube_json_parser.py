import logging
import re
from datetime import date
from typing import Optional, Set

from model.record import Record
from parser.chinese_num_parser import ChineseNumParser
from parser.programme_name_parser import ProgrammeNameParser


class YoutubeJsonParser:
    def __init__(self, programme_names: Set[str]):
        self._chinese_num_parser = ChineseNumParser()
        self._programme_name_parser = ProgrammeNameParser()
        self._programme_names_sorted_desc_by_length = list(sorted(programme_names, key=len, reverse=True))

    def parse_json(self, j: dict) -> Record:
        programme = self._try_extract_programme(j)
        return Record(
            programme=programme,
            episode=self._try_extract_episode(j),
            dt=self._try_extract_date(j),
            title=self._clean_up_title(j, programme),
            description=j["description"],
            youtube_id=j["id"],
            youtube_fulltitle=j["fulltitle"],
            youtube_categories=j["categories"],
            youtube_tags=j["tags"],
            youtube_upload_date=j["upload_date"],
        )

    def _try_extract_date(self, j: dict) -> Optional[date]:
        def _remove_spaces(s: str) -> str:
            return re.sub(r'\s+', '', s)

        title = _remove_spaces(j["fulltitle"])

        match = re.search(r'(\d{1,2})[-_./ ](\d{1,2})[-_./ ](\d{4})', title)
        if match:
            day, month, year = tuple(map(int, match.groups()))
            try:
                return date(year=year, month=month, day=day)
            except ValueError:
                # Some odd cases e.g. 5-15-2014 議事論事 第三十二集-sVNrYi8kS8c.info.json
                return date(year=year, month=day, day=month)

        match = re.search(r'(\d{4})[-_./ ](\d{1,2})[-_./ ](\d{1,2})', title)
        if match:
            year, month, day = tuple(map(int, match.groups()))
            return date(year=year, month=month, day=day)

        match = re.search(r'(\d{1,2})[-_./ ](\d{1,2})[-_./ ](\d{1,2})', title)
        if match:
            d1, month, d2 = tuple(map(int, match.groups()))
            year = int(j["upload_date"][:4])  # assume video was uploaded in the same year to Youtube
            if year % 100 == d1:
                return date(year=year, month=month, day=d2)
            elif year % 100 == d2:
                return date(year=year, month=month, day=d1)

        match = re.search(r'(\d+)年(\d+)月(\d+)日', title)
        if match:
            year, month, day = tuple(map(int, match.groups()))
            return date(year=year, month=month, day=day)

        match = re.search(r'(\d+)月(\d+)日', title)
        if match:
            month, day = tuple(map(int, match.groups()))
            year = int(j["upload_date"][:4])  # assume video was uploaded in the same year to Youtube
            return date(year=year, month=month, day=day)

        return None

    def _try_extract_programme(self, j: dict) -> Optional[str]:
        def _normalize_spaces(title: str) -> str:
            return re.sub(r'\s+', ' ', title)

        title = _normalize_spaces(j["fulltitle"].strip())
        # TODO: This is O(n^2) search, speed this up.
        for programme_name in self._programme_names_sorted_desc_by_length:
            if programme_name in title:
                return programme_name
        return None

    def _try_extract_episode(self, j: dict) -> Optional[int]:
        match = re.search(r'第\s*(\d+?)\s*集', j["fulltitle"])
        if match:
            episode = int(match.group(1))
            return episode

        match = re.search(r'第\s*([^第]+?)\s*集', j["fulltitle"])
        if match:
            try:
                episode = self._chinese_num_parser.parse(match.group(1))
                return episode
            except Exception:
                logging.warning(f"Failed to parse episode for title: {j['fulltitle']}", exc_info=True)

        return None

    def _clean_up_title(self, j: dict, programme: Optional[str]) -> str:
        def _normalize_spaces(title: str) -> str:
            return re.sub(r'\s+', ' ', title)

        def _remove_date(title: str) -> str:
            for pattern in (
                    r'\d{1,2}[-_./ ]\d{1,2}[-_./ ]\d{4}',
                    r'\d{4}[-_./ ]\d{1,2}[-_./ ]\d{1,2}',
                    r'\d{1,2}[-_./ ]\d{1,2}[-_./ ]\d{2}',
                    r'\d{2}[-_./ ]\d{1,2}[-_./ ]\d{1,2}',
            ):
                title = re.sub(pattern, '', title)
            return title

        def _remove_programme_name(title: str):
            title = re.sub(
                rf'[《【「]?\s*{re.escape(programme)}\s*[》】」：–:-]*',
                '',
                title)
            # remove leftover punctuations
            return title

        title = _normalize_spaces(j["fulltitle"].strip())
        title = _remove_date(title)
        if programme:
            title = _remove_programme_name(title)
        return title.strip()
