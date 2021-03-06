import asyncio

import pytest

from crawler.podcast.episode_list_crawler import EpisodeListCrawler


@pytest.mark.asyncio
async def test_list_all_episodes():
    crawler = EpisodeListCrawler(asyncio.Semaphore(100))
    pid, episodes = await crawler.list_all_episodes(pid=256)
    assert pid == 244
    assert len(episodes) >= 555
