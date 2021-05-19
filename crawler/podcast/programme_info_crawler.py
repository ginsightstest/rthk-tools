import asyncio

from bs4 import BeautifulSoup

from crawler.podcast import client
from model.podcast.programme import ProgrammeInfo


class ProgrammeInfoCrawler:
    def __init__(self, sem: asyncio.Semaphore):
        self._sem = sem

    async def get_programme_info(self, pid: int) -> ProgrammeInfo:
        html = await client.get(
            f'https://podcast.rthk.hk/podcast/item.php?pid={pid}',
            sem=self._sem)
        soup = BeautifulSoup(html, features="lxml")
        title = soup.select_one(
            '#prog-detail > div > div.prog-box > div.prog-box-title > div.prog-title > h2').get_text()
        description = soup.select_one('#prog-detail > div > div.tab-box-about > div').get_text()
        language = soup.select_one(
            '#prog-detail > div > div.prog-box > div.prog-box-info > ul > li:nth-child(2) > span').get_text()
        category_divs = soup.select(
            '#prog-detail > div > div.prog-box > div.prog-box-info > ul > li:nth-child(3) > a')
        category_names = [div.get_text() for div in category_divs]
        rss_url = soup.select_one(
            '#prog-detail > div > div.prog-box > div.subscribe-divs > div > div > a:nth-child(2)')['href']
        return ProgrammeInfo(
            pid=pid,
            title=title,
            description=description,
            language=language,
            category_names=category_names,
            rss_url=rss_url
        )
