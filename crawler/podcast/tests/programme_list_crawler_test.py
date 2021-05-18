import asyncio

import pytest

from crawler.podcast.programme_list_crawler import ProgrammeListCrawler


@pytest.mark.asyncio
async def test_list_programme():
    crawler = ProgrammeListCrawler(asyncio.Semaphore(100))
    chinese_programmes = await crawler.list_programmes(language='zh-CN')
    english_programmes = await crawler.list_programmes(language='en-US')
    assert len(chinese_programmes) == 1011
    assert len(english_programmes) == 230
