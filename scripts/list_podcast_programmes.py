import argparse
import asyncio
from dataclasses import dataclass
from typing import List

from crawler.podcast.episode_list_crawler import Episode, EpisodeListCrawler
from crawler.podcast.programme_list_crawler import ProgrammeListCrawler
from scripts.args import Args
from util.lists import flatten
from writer.episodes_csv_writer import EpisodesCsvWriter


@dataclass
class ListPodcastProgrammesArgs(Args):
    csv_out: str
    parallelism: int
    pids: List[int]


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--csv-out', required=True, help='Path for output csv file')
    parser.add_argument('--parallelism', type=int, default=100, help='How many HTTP requests in parallel')
    parser.add_argument('--pid', nargs='*', type=int, default=[], help='pids to crawl')


def parse_args(raw_args: argparse.Namespace) -> ListPodcastProgrammesArgs:
    csv_out = raw_args.csv_out
    parallelism = raw_args.parallelism
    pid = raw_args.pid

    return ListPodcastProgrammesArgs(
        csv_out=csv_out,
        parallelism=parallelism,
        pids=pid
    )


def run(args: ListPodcastProgrammesArgs):
    episodes = asyncio.run(_crawl_podcast_site(parallelism=args.parallelism, pids=args.pids))
    EpisodesCsvWriter(episodes).write_to_csv(args.csv_out)


async def _crawl_podcast_site(parallelism: int, pids: List[int]) -> List[Episode]:
    sem = asyncio.Semaphore(parallelism)

    if not pids:
        programmes = await ProgrammeListCrawler(sem).list_programmes()
        pids = [programme.pid for programme in programmes]

    episode_crawler = EpisodeListCrawler(sem)
    nested_episodes = await asyncio.gather(*map(episode_crawler.list_all_episodes, pids))
    episodes = flatten(nested_episodes)
    return episodes
