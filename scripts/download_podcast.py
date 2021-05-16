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
    years: List[int]
    parallelism: int


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--out-dir', required=True, help='Directory to store downloaded files')
    parser.add_argument('--csv-in', required=True, help='Path to podcast list csv')
    parser.add_argument('--pid', nargs='+', type=int, help='pids to download')
    parser.add_argument('--year', nargs='*', type=int, default=[], help='restrict to years')
    parser.add_argument('--parallelism', type=int, default=20, help='How many HTTP requests in parallel')


def parse_args(raw_args: argparse.Namespace) -> DownloadPodcastArgs:
    out_dir = to_abs_path(raw_args.out_dir)
    csv_in = to_abs_path(raw_args.csv_in)
    pid = raw_args.pid
    years = raw_args.year
    parallelism = raw_args.parallelism

    return DownloadPodcastArgs(
        out_dir=out_dir,
        csv_in=csv_in,
        pids=pid,
        years=years,
        parallelism=parallelism
    )


def run(args: DownloadPodcastArgs):
    asyncio.run(
        _download_and_save_podcast(
            args
        )
    )


async def _download_and_save_podcast(args: DownloadPodcastArgs):
    sem = asyncio.Semaphore(args.parallelism)
    episodes = _filter_episodes_from_csv(pids=args.pids, years=args.years, csv_in=args.csv_in)
    m3u8_episodes, mp4_episodes = [], []
    for e in episodes:
        if e.m3u8_url:
            m3u8_episodes.append(e)
        else:
            mp4_episodes.append(e)

    await _download_and_save_m3u8(m3u8_episodes, out_dir=args.out_dir, sem=sem)
    await _download_and_save_mp4(mp4_episodes, out_dir=args.out_dir, sem=sem)


def _filter_episodes_from_csv(pids: List[int], years: List[int], csv_in: str) -> List[Episode]:
    def _matches_criteria(episode: Episode) -> bool:
        if not episode.pid in pids:
            return False
        if years and not episode.episode_date.year in years:
            return False
        return True

    episodes = EpisodesCsvReader().read_to_episodes(csv_in)
    matching_episodes = list(filter(_matches_criteria, episodes))
    logging.info(f'Will download episodes: {matching_episodes}')
    return matching_episodes


async def _download_and_save_m3u8(m3u8_episodes: List[Episode], out_dir: str, sem: asyncio.Semaphore):
    m3u8_downloader = M3U8Downloader(sem=sem)

    async def _download(episode: Episode):
        filename = f'rthk_{episode.pid}_{episode.eid}.mp4'
        out_path = os.path.join(out_dir, filename)
        await m3u8_downloader.save_download(episode.m3u8_url, out_path=out_path)

    for episode in m3u8_episodes:
        await _download(episode)


async def _download_and_save_mp4(mp4_episodes: List[Episode], out_dir: str, sem: asyncio.Semaphore):
    mp4_downloader = Mp4Downloader(sem=sem)

    async def _download(episode: Episode):
        filename = f'rthk_{episode.pid}_{episode.eid}.mp4'
        out_path = os.path.join(out_dir, filename)
        await mp4_downloader.save_download(episode.file_url, out_path=out_path)

    await asyncio.gather(*map(_download, mp4_episodes))
