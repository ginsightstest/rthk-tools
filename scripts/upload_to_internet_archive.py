import argparse
import glob
import os
import re
from dataclasses import dataclass
from typing import List

from csv_reader_writer.episodes_csv_reader import EpisodesCsvReader
from model.internetarchive.upload import InternetArchiveUploadApiRequest
from scripts.args import Args
from uploader.internet_archive_uploader import InternetArchiveUploader
from util.paths import to_abs_path


@dataclass
class UploadToInternetArchiveArgs(Args):
    upload_dir: str
    csv_in: str
    with_date: bool


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--upload-dir', required=True, help='Directory containing video files to upload')
    parser.add_argument('--csv-in', required=True, help='Path to podcast list csv')
    parser.add_argument('--with-date', default=False, action='store_true', help='Whether to add date to title')


def parse_args(raw_args: argparse.Namespace) -> UploadToInternetArchiveArgs:
    upload_dir = to_abs_path(raw_args.upload_dir)
    csv_in = to_abs_path(raw_args.csv_in)
    with_date = raw_args.with_date

    return UploadToInternetArchiveArgs(
        upload_dir=upload_dir,
        csv_in=csv_in,
        with_date=with_date
    )


def run(args: UploadToInternetArchiveArgs):
    _upload_to_internet_archive(args)


def _upload_to_internet_archive(args: UploadToInternetArchiveArgs):
    publish_requests = _build_publish_requests(args)
    internet_archive_uploader = InternetArchiveUploader()
    for publish_request in publish_requests:
        internet_archive_uploader.upload(publish_request)


def _build_publish_requests(args: UploadToInternetArchiveArgs) -> List[InternetArchiveUploadApiRequest]:
    episodes = EpisodesCsvReader().read_to_episodes(args.csv_in)
    episodes_by_pid_eid = {(e.pid, e.eid): e for e in episodes}
    publish_requests = []
    for path in sorted(glob.iglob(os.path.join(args.upload_dir, 'rthk_*_*.*')), reverse=True):
        filename = path.rsplit('/', 1)[1]
        match = re.fullmatch(r'rthk_(\d+)_(\d+)\.[^.]+', filename)
        if match:
            pid, eid = int(match.group(1)), int(match.group(2))
            episode = episodes_by_pid_eid[(pid, eid)]
            programme_name_eng = re.fullmatch(r'https://podcast.rthk.hk/podcast/(.+)\.xml', episode.rss_url) \
                .group(1) \
                .replace('_', '-')
            date_str = episode.episode_date.strftime('%Y-%m-%d')
            mediatype = 'movies' if episode.format == 'video' else 'audio'
            if args.with_date:
                title = f'{episode.og_title} | {date_str}'
            else:
                title = episode.og_title
            publish_request = InternetArchiveUploadApiRequest(
                identifier=f'rthk-podcast-{programme_name_eng}-{date_str}',
                title=title,
                description=episode.og_description,
                mediatype=mediatype,
                file_path=path,
                collection=f'opensource_{mediatype}',
                creator='Radio Television Hong Kong',
                date=date_str)
            publish_requests.append(publish_request)
    return publish_requests
