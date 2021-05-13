import asyncio

import pytest

from crawler.podcast.programme_list_crawler import ProgrammeListCrawler


@pytest.mark.asyncio
async def test_list_programme():
    crawler = ProgrammeListCrawler(asyncio.Semaphore(100))
    programmes = await crawler.list_programmes()
    assert len(programmes) == 1010
