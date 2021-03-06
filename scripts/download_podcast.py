import argparse
import asyncio
import logging
import os
from dataclasses import dataclass
from typing import List

from csv_reader_writer.episodes_csv_reader import EpisodesCsvReader
from downloader.M3U8Downloader import M3U8Downloader
from downloader.Mp4Downloader import Mp4Downloader
from model.podcast.episode import Episode
from scripts.args import Args
from util.paths import to_abs_path


@dataclass
class DownloadPodcastArgs(Args):
    out_dir: str
    csv_in: str
    pids: List[int]
    eids: List[int]
    years: List[int]
    parallelism: int
    force_mp4: bool


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--out-dir', required=True, help='Directory to store downloaded files')
    parser.add_argument('--csv-in', required=True, help='Path to podcast list csv')
    parser.add_argument('--pid', nargs='+', action='extend', type=int, help='pids to download')

    eids_or_years = parser.add_mutually_exclusive_group()
    eids_or_years.add_argument('--eid', nargs='+', action='extend', type=int, default=[], help='eids to download')
    eids_or_years.add_argument('--year', nargs='*', action='extend', type=int, default=[], help='restrict to years')

    parser.add_argument('--parallelism', type=int, default=100, help='How many HTTP requests in parallel')
    parser.add_argument('--force-mp4', default=False, action='store_true', help='Skip m3u8, force download mp4')


def parse_args(raw_args: argparse.Namespace) -> DownloadPodcastArgs:
    out_dir = to_abs_path(raw_args.out_dir)
    csv_in = to_abs_path(raw_args.csv_in)
    pid = raw_args.pid
    eid = raw_args.eid
    years = raw_args.year
    parallelism = raw_args.parallelism
    force_mp4 = raw_args.force_mp4

    return DownloadPodcastArgs(
        out_dir=out_dir,
        csv_in=csv_in,
        pids=pid,
        eids=eid,
        years=years,
        parallelism=parallelism,
        force_mp4=force_mp4
    )


def run(args: DownloadPodcastArgs):
    asyncio.run(
        _download_and_save_podcast(
            args
        )
    )


async def _download_and_save_podcast(args: DownloadPodcastArgs):
    sem = asyncio.Semaphore(args.parallelism)
    episodes = _filter_episodes_from_csv(pids=args.pids, eids=args.eids, years=args.years, csv_in=args.csv_in)

    m3u8_episodes, mp4_episodes = [], []
    for e in episodes:
        if e.m3u8_url and not args.force_mp4:
            m3u8_episodes.append(e)
        elif e.file_url:
            mp4_episodes.append(e)

    failed_episodes = await _download_and_save_m3u8(m3u8_episodes, out_dir=args.out_dir, sem=sem)
    mp4_episodes += failed_episodes
    await _download_and_save_mp4(mp4_episodes, out_dir=args.out_dir, sem=sem)


def _filter_episodes_from_csv(pids: List[int], eids: List[int], years: List[int], csv_in: str) -> List[Episode]:
    def _matches_criteria(episode: Episode) -> bool:
        if not episode.pid in pids:
            return False
        if eids and not episode.eid in eids:
            return False
        if years and not episode.episode_date.year in years:
            return False
        return True

    episodes = EpisodesCsvReader().read_to_episodes(csv_in)
    matching_episodes = list(filter(_matches_criteria, episodes))
    logging.info(f'Will download episodes: {matching_episodes}')
    return matching_episodes


async def _download_and_save_m3u8(episodes: List[Episode], out_dir: str, sem: asyncio.Semaphore) -> List[Episode]:
    m3u8_downloader = M3U8Downloader(sem=sem)

    async def _download(episode: Episode):
        filename = f'rthk_{episode.pid}_{episode.eid}.mp4'
        out_path = os.path.join(out_dir, filename)
        await m3u8_downloader.save_download(episode.m3u8_url, out_path=out_path)

    failed_episodes = []

    for episode in episodes:
        try:
            await _download(episode)
        except Exception:
            logging.warning(
                f'Failed to download m3u8 for episode pid={episode.pid} eid={episode.eid}, will fall back to mp4',
                exc_info=True)
            failed_episodes.append(episode)

    return failed_episodes


async def _download_and_save_mp4(episodes: List[Episode], out_dir: str, sem: asyncio.Semaphore):
    mp4_downloader = Mp4Downloader(sem=sem)

    async def _download(episode: Episode, tqdm_local_position: int):
        basename, ext = os.path.splitext(episode.file_url)
        filename = f'rthk_{episode.pid}_{episode.eid}{ext}'
        out_path = os.path.join(out_dir, filename)
        await mp4_downloader.save_download(episode.file_url, out_path=out_path, tqdm_local_position=tqdm_local_position)

    await asyncio.gather(*[_download(episode, i) for i, episode in enumerate(episodes)])
