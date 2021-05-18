import asyncio
import logging
import math
from typing import List

import xmltodict

from crawler.podcast import client
from model.podcast.programme import Programme
from util.lists import flatten


class ProgrammeListCrawler:
    def __init__(self, sem: asyncio.Semaphore):
        self._sem = sem

    async def list_programmes(self, language: str) -> List[Programme]:
        async def _get_total_pages() -> int:
            xml = await client.get(
                f'https://podcast.rthk.hk/podcast/programmeList.php?type=all&page=1&order=hot&lang={language}',
                sem=self._sem)
            root = xmltodict.parse(xml)
            total_series = int(root['programmeList']['total'])
            programme_per_page = int(root['programmeList']['programmePerPage'])
            total_pages = math.ceil(total_series / programme_per_page)
            logging.debug(f'Total num of programme pages: {total_pages}')
            return total_pages

        async def _list_programmes_in_page(page: int) -> List[Programme]:
            xml = await client.get(
                f'https://podcast.rthk.hk/podcast/programmeList.php?type=all&page={page}&order=hot&lang={language}',
                sem=self._sem)
            root = xmltodict.parse(xml, force_list={'programme'})
            logging.debug(f'Got programmes in page: {page}')
            return [Programme(
                pid=int(p['link'].removeprefix('item.php?pid=')),
                title=p['title'],
                format=p['format']
            ) for p in root['programmeList']['programme']]

        total_pages = await _get_total_pages()
        nested_programmes = await asyncio.gather(*map(_list_programmes_in_page, range(1, total_pages + 1)))
        return flatten(nested_programmes)
