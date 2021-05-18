import asyncio
import logging
import re
from typing import Dict, List

import xmltodict
from bs4 import BeautifulSoup

from crawler.podcast import client
from model.podcast.episode import Episode
from util.dates import duration_to_seconds, ymd_to_date
from util.lists import flatten


class EpisodeListCrawler:
    def __init__(self, sem: asyncio.Semaphore):
        self._sem = sem

    async def list_all_episodes(self, pid: int) -> List[Episode]:
        logging.info(f'Crawling pid {pid}...')
        years = await self._list_available_years(pid)

        episodes_from_xml = await self._list_episodes_xml(pid, years)
        eids = [episode.eid for episode in episodes_from_xml]

        episodes_from_html = await self._list_episodes_html(pid, eids)

        def _merge(episodes_from_xml: List[Episode], episodes_from_html: List[Episode]) -> List[Episode]:
            xml_episodes_grouped_by_pid_eid: Dict[(int, int), Episode] = {(e.pid, e.eid): e for e in episodes_from_xml}
            html_episodes_grouped_by_pid_eid: Dict[(int, int), Episode] = {(e.pid, e.eid): e for e in
                                                                           episodes_from_html}
            merged_episodes = [
                Episode(
                    pid=pid,
                    eid=eid,
                    programme_title=html_episodes_grouped_by_pid_eid[(pid, eid)].programme_title,
                    episode_title=xml_episodes_grouped_by_pid_eid[(pid, eid)].episode_title,
                    episode_date=xml_episodes_grouped_by_pid_eid[(pid, eid)].episode_date,
                    duration_seconds=xml_episodes_grouped_by_pid_eid[(pid, eid)].duration_seconds,
                    og_title=html_episodes_grouped_by_pid_eid[(pid, eid)].og_title,
                    og_description=html_episodes_grouped_by_pid_eid[(pid, eid)].og_description,
                    cids=html_episodes_grouped_by_pid_eid[(pid, eid)].cids,
                    category_names=html_episodes_grouped_by_pid_eid[(pid, eid)].category_names,
                    file_url=xml_episodes_grouped_by_pid_eid[(pid, eid)].file_url,
                    m3u8_url=html_episodes_grouped_by_pid_eid[(pid, eid)].m3u8_url,
                    rss_url=html_episodes_grouped_by_pid_eid[(pid, eid)].rss_url,
                    format=xml_episodes_grouped_by_pid_eid[(pid, eid)].format
                ) for (pid, eid) in xml_episodes_grouped_by_pid_eid.keys()
            ]
            return merged_episodes

        all_episodes = _merge(
            episodes_from_xml,
            episodes_from_html
        )
        logging.info(f'Got {len(all_episodes)} episodes for pid {pid}')
        return all_episodes

    async def _list_available_years(self, pid: int) -> List[int]:
        html = await client.get(
            f'https://podcast.rthk.hk/podcast/item.php?pid={pid}',
            sem=self._sem
        )
        soup = BeautifulSoup(html, features="lxml")
        years = [int(option['value'])
                 for option in soup.select('#switch-years > option')
                 if int(option['value']) > 1900]  # Filter out invalid years, e.g. 0000
        logging.debug(f'pid {pid} has available years: {years}')
        return years

    async def _list_episodes_xml(self, pid: int, years: List[int]) -> List[Episode]:
        async def _list_episodes_in_year(year: int) -> List[Episode]:
            xml = await client.get(
                f'https://podcast.rthk.hk/podcast/episodeList.php?pid={pid}&year={year}&display=all',
                sem=self._sem)
            root = xmltodict.parse(xml, force_list={'episode'})
            return [Episode(
                pid=int(e['pid']),
                eid=int(e['eid']),
                episode_title=e['episodeTitle'],
                episode_date=ymd_to_date(e['episodeDate']),
                duration_seconds=duration_to_seconds(e['duration']),
                file_url=e['mediafile'],
                format=e['format']
            ) for e in root['episodeList']['episode']]

        nested_episodes = await asyncio.gather(*map(_list_episodes_in_year, years))
        episodes = flatten(nested_episodes)
        logging.debug(f'Found {len(episodes)} xml episodes for pid {pid} and years {years}')
        return episodes

    async def _list_episodes_html(self, pid: int, eids: List[int]) -> List[Episode]:
        async def _get_episode_info(eid: int) -> Episode:
            html = await client.get(
                f'https://podcast.rthk.hk/podcast/item.php?pid={pid}&eid={eid}',
                sem=self._sem)
            soup = BeautifulSoup(html, features="lxml")
            try:
                programme_title = soup.select_one(
                    '#prog-detail > div > div.prog-box > div.prog-box-title > div.prog-title > h2').get_text()
                og_title = soup.select_one('meta[property="og:title"]')['content']
                og_description = soup.select_one('meta[property="og:description"]')['content']
                category_divs = soup.select(
                    '#prog-detail > div > div.prog-box > div.prog-box-info > ul > li:nth-child(3) > a')
                cids = [int(re.fullmatch(r'category.php\?cid=(\d+)&lang=.*', div['href']).group(1))
                        for div in category_divs]
                category_names = [div.get_text() for div in category_divs]
                m3u8_url = re.search(f'[^"]+\.m3u8', html) and re.search(f'[^"]+\.m3u8', html).group()
                rss_url = soup.select_one(
                    '#prog-detail > div > div.prog-box > div.subscribe-divs > div > div > a:nth-child(2)')['href']
                logging.debug(f'Got html episode info for (pid, eid) = ({pid}, {eid})')
                return Episode(
                    pid=pid,
                    eid=eid,
                    programme_title=programme_title,
                    og_title=og_title,
                    og_description=og_description,
                    cids=cids,
                    category_names=category_names,
                    m3u8_url=m3u8_url,
                    rss_url=rss_url
                )
            except:
                logging.warning(f'Failed to get html episode info for (pid, eid) = ({pid}, {eid})', exc_info=True)
                return Episode(
                    pid=pid,
                    eid=eid,
                    programme_title=None,
                    og_title=None,
                    og_description=None,
                    cids=None,
                    category_names=None,
                    m3u8_url=None,
                    rss_url=None
                )

        episodes = await asyncio.gather(*map(_get_episode_info, eids))
        return list(episodes)
