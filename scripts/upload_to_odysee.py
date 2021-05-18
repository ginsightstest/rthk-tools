import argparse
import asyncio
import glob
import os
import re
from dataclasses import dataclass
from typing import List

from csv_reader_writer.episodes_csv_reader import EpisodesCsvReader
from model.odysee.publish import OdyseePublishApiRequest
from scripts.args import Args
from uploader.OdyseeUploader import OdyseeUploader
from util.paths import to_abs_path


@dataclass
class UploadToOdyseeArgs(Args):
    upload_dir: str
    csv_in: str
    channel_id: str
    bid: float


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--upload-dir', required=True, help='Directory containing video files to upload')
    parser.add_argument('--csv-in', required=True, help='Path to podcast list csv')
    parser.add_argument('--channel-id', required=True, help='Odysee channel id')
    parser.add_argument('--bid', type=str, default="0.001", help='Odysee bid')


def parse_args(raw_args: argparse.Namespace) -> UploadToOdyseeArgs:
    upload_dir = to_abs_path(raw_args.upload_dir)
    csv_in = to_abs_path(raw_args.csv_in)
    channel_id = raw_args.channel_id
    bid = raw_args.bid

    return UploadToOdyseeArgs(
        upload_dir=upload_dir,
        csv_in=csv_in,
        channel_id=channel_id,
        bid=bid,
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


def _build_publish_requests(args: UploadToOdyseeArgs) -> List[OdyseePublishApiRequest]:
    episodes = EpisodesCsvReader().read_to_episodes(args.csv_in)
    episodes_by_pid_eid = {(e.pid, e.eid): e for e in episodes}
    publish_requests = []
    for path in glob.iglob(os.path.join(args.upload_dir, 'rthk_*_*.*')):
        filename = path.rsplit('/', 1)[1]
        match = re.fullmatch(r'rthk_(\d+)_(\d+)\.[^.]+', filename)
        if match:
            pid, eid = int(match.group(1)), int(match.group(2))
            episode = episodes_by_pid_eid[(pid, eid)]
            programme_name_eng = re.fullmatch(r'https://podcast.rthk.hk/podcast/(.+)\.xml', episode.rss_url) \
                .group(1) \
                .replace('_', '-')
            date_str = episode.episode_date.strftime('%Y-%m-%d')
            publish_request = OdyseePublishApiRequest(
                name=f'{programme_name_eng}-{date_str}',
                title=episode.og_title,
                description=episode.og_description,
                file_path=path,
                channel_id=args.channel_id,
                bid=args.bid,
                tags=episode.category_names
            )
            publish_requests.append(publish_request)
    return publish_requests