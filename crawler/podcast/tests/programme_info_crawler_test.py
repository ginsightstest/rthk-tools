import asyncio

import pytest

from crawler.podcast.programme_info_crawler import ProgrammeInfoCrawler


@pytest.mark.asyncio
async def test_list_programme():
    crawler = ProgrammeInfoCrawler(asyncio.Semaphore())
    programme_info = await crawler.get_programme_info(244)
    assert programme_info.pid == 244
    assert programme_info.title == '鏗鏘集'
    assert '《鏗鏘集》是一個屬於觀眾的節目。 ' in programme_info.description
    assert programme_info.language == '中文'
    assert programme_info.rss_url == 'https://podcast.rthk.hk/podcast/hongkongconnection_i.xml'
