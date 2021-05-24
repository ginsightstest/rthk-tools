import argparse
import asyncio
import collections
import glob
import os
import re
from dataclasses import dataclass
from typing import List

from csv_reader_writer.episodes_csv_reader import EpisodesCsvReader
from model.odysee.publish import OdyseePublishApiRequest
from scripts.args import Args
from uploader.odysee_uploader import OdyseeUploader
from util.paths import to_abs_path


@dataclass
class UploadToOdyseeArgs(Args):
    upload_dir: str
    csv_in: str
    channel_id: str
    bid: str
    with_date: bool
    pause: int


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--upload-dir', required=True, help='Directory containing video files to upload')
    parser.add_argument('--csv-in', required=True, help='Path to podcast list csv')
    parser.add_argument('--channel-id', required=True, help='Odysee channel id')
    parser.add_argument('--bid', type=str, default="0.001", help='Odysee bid')
    parser.add_argument('--with-date', default=False, action='store_true', help='Whether to add date to title')
    parser.add_argument('--pause', type=float, default=0.0, help='How many seconds to pause in between uploads')


def parse_args(raw_args: argparse.Namespace) -> UploadToOdyseeArgs:
    upload_dir = to_abs_path(raw_args.upload_dir)
    csv_in = to_abs_path(raw_args.csv_in)
    channel_id = raw_args.channel_id
    bid = raw_args.bid
    with_date = raw_args.with_date
    pause = raw_args.pause

    return UploadToOdyseeArgs(
        upload_dir=upload_dir,
        csv_in=csv_in,
        channel_id=channel_id,
        bid=bid,
        with_date=with_date,
        pause=pause
    )


def run(args: UploadToOdyseeArgs):
    asyncio.run(
        _upload_to_odysee(
            args
        )
    )


async def _upload_to_odysee(args: UploadToOdyseeArgs):
    publish_requests = _build_publish_requests(args)
    odysee_uploader = OdyseeUploader()
    for publish_request in publish_requests:
        await odysee_uploader.upload(publish_request)
        await asyncio.sleep(args.pause)


def _build_publish_requests(args: UploadToOdyseeArgs) -> List[OdyseePublishApiRequest]:
    episodes = EpisodesCsvReader().read_to_episodes(args.csv_in)
    episodes_by_pid_eid = {(e.pid, e.eid): e for e in episodes}
    date_collision_counter = collections.Counter()
    publish_requests = []
    for path in sorted(glob.iglob(os.path.join(args.upload_dir, 'rthk_*_*.*')), reverse=True):
        filename = path.rsplit('/', 1)[1]
        match = re.fullmatch(r'rthk_(\d+)_(\d+)\.[^.]+', filename)
        if match:
            pid, eid = int(match.group(1)), int(match.group(2))
            episode = episodes_by_pid_eid[(pid, eid)]
            date_collision_counter[episode.episode_date] += 1

            programme_name_eng = re.fullmatch(r'https://podcast.rthk.hk/podcast/(.+)\.xml', episode.rss_url) \
                .group(1) \
                .replace('_', '-')
            date_str = episode.episode_date.strftime('%Y-%m-%d')

            name = f'{programme_name_eng}-{date_str}'
            if date_collision_counter[episode.episode_date] > 1:
                name = f'{name}-{date_collision_counter[episode.episode_date]}'

            if args.with_date:
                title = f'{episode.og_title} | {date_str}'
            else:
                title = episode.og_title

            publish_request = OdyseePublishApiRequest(
                name=name,
                title=title,
                description=episode.og_description,
                file_path=path,
                channel_id=args.channel_id,
                bid=args.bid,
                tags=['RTHK', episode.programme_title] + episode.category_names,
                thumbnail_url=f'https://podcast.rthk.hk/podcast/upload_photo/item_photo/170x170_{pid}.jpg',
                languages=['en'] if episode.language == '英文' else ['zh-HK']
            )
            publish_requests.append(publish_request)
    return publish_requests
