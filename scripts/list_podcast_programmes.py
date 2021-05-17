import argparse
import asyncio
import glob
import logging
import os
import re
from dataclasses import dataclass
from typing import List

import tqdm

from crawler.podcast.episode_list_crawler import EpisodeListCrawler
from crawler.podcast.programme_list_crawler import ProgrammeListCrawler
from csv_reader_writer.episodes_csv_reader import EpisodesCsvReader
from csv_reader_writer.episodes_csv_writer import EpisodesCsvWriter
from model.podcast.episode import Episode
from scripts.args import Args
from util.paths import to_abs_path

UNSUPPORTED_PIDS = {
    113  # 視像新聞
}


@dataclass
class ListPodcastProgrammesArgs(Args):
    csv_out: str
    incremental: bool
    parallelism: int
    pids: List[int]


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--csv-out', required=True, help='Path for output csv file')
    parser.add_argument('--incremental', default=False, action='store_true', help='Whether to save csvs per pid')
    parser.add_argument('--parallelism', type=int, default=100, help='How many HTTP requests in parallel')
    parser.add_argument('--pid', nargs='*', type=int, default=[], help='pids to crawl')


def parse_args(raw_args: argparse.Namespace) -> ListPodcastProgrammesArgs:
    csv_out = raw_args.csv_out
    incremental = raw_args.incremental
    parallelism = raw_args.parallelism
    pid = raw_args.pid

    return ListPodcastProgrammesArgs(
        csv_out=to_abs_path(csv_out),
        incremental=incremental,
        parallelism=parallelism,
        pids=pid
    )


def run(args: ListPodcastProgrammesArgs):
    asyncio.run(
        _crawl_and_save_podcast_site(
            args
        )
    )


async def _crawl_and_save_podcast_site(args: ListPodcastProgrammesArgs):
    sem = asyncio.Semaphore(args.parallelism)
    working_dir = to_abs_path(os.path.join(args.csv_out, '..'))

    pids_to_crawl = await _determine_pids_to_crawl(args.pids, working_dir=working_dir, sem=sem)
    logging.info(f'Will crawl pids: {pids_to_crawl}...')

    episode_crawler = EpisodeListCrawler(sem)
    all_episodes = []
    with tqdm.tqdm(total=len(pids_to_crawl)) as progress_bar:
        for pid in pids_to_crawl:
            episodes_for_pid = await episode_crawler.list_all_episodes(pid)
            if args.incremental:
                EpisodesCsvWriter(episodes_for_pid) \
                    .write_to_csv(
                    to_abs_path(os.path.join(args.csv_out, '..', f'{pid}.rthk.tmp.csv')))
            else:
                all_episodes.extend(episodes_for_pid)
            progress_bar.update(1)

    if args.incremental:
        all_episodes = _combine_incremental_csvs(working_dir)
    EpisodesCsvWriter(all_episodes).write_to_csv(args.csv_out)


async def _determine_pids_to_crawl(pids: List[int], working_dir: os.path, sem: asyncio.Semaphore) -> List[int]:
    if not pids:
        programmes = await ProgrammeListCrawler(sem).list_programmes()
        pids = [programme.pid for programme in programmes]

    already_done_pids = [
        int(re.search("(\d+)\.rthk\.tmp\.csv", filename).group(1))
        for filename in glob.iglob(os.path.join(working_dir, "*.rthk.tmp.csv"))
    ]

    pids = list(set(pids) - set(already_done_pids) - UNSUPPORTED_PIDS)
    return pids


def _combine_incremental_csvs(working_dir) -> List[Episode]:
    all_episodes = []
    reader = EpisodesCsvReader()
    for filename in glob.iglob(os.path.join(working_dir, "*.rthk.tmp.csv")):
        episodes = reader.read_to_episodes(filename)
        all_episodes.extend(episodes)
    return all_episodes
