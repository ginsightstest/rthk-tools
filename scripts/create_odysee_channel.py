import argparse
import asyncio
import re
from dataclasses import dataclass

from crawler.podcast.programme_info_crawler import ProgrammeInfoCrawler
from model.odysee.publish import OdyseeChannelCreateApiRequest
from model.podcast.programme import ProgrammeInfo
from scripts.args import Args
from uploader.OdyseeUploader import OdyseeUploader


@dataclass
class CreateOdyseeChannelArgs(Args):
    pid: int
    bid: str


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--pid', required=True, type=int, help='pid to create channel for')
    parser.add_argument('--bid', type=str, default="0.001", help='Odysee bid')


def parse_args(raw_args: argparse.Namespace) -> CreateOdyseeChannelArgs:
    pid = raw_args.pid
    bid = raw_args.bid

    return CreateOdyseeChannelArgs(
        pid=pid,
        bid=bid,
    )


def run(args: CreateOdyseeChannelArgs):
    asyncio.run(
        _create_odysee_channel(
            args
        )
    )


async def _create_odysee_channel(args: CreateOdyseeChannelArgs):
    sem = asyncio.Semaphore()
    programme_info = await ProgrammeInfoCrawler(sem=sem).get_programme_info(args.pid)
    channel_create_request = _build_channel_create_request(args, programme_info)
    await OdyseeUploader().create_channel(channel_create_request)


def _build_channel_create_request(args: CreateOdyseeChannelArgs,
                                  programme_info: ProgrammeInfo) -> OdyseeChannelCreateApiRequest:
    programme_name_eng = re.fullmatch(r'https://podcast.rthk.hk/podcast/(.+)\.xml', programme_info.rss_url) \
        .group(1) \
        .replace('_', '-')
    return OdyseeChannelCreateApiRequest(
        name=f'@RTHKBackup-{programme_name_eng}',
        title=programme_info.title,
        description=programme_info.description,
        bid=args.bid,
        tags=['RTHK', programme_info.title] + programme_info.category_names,
        thumbnail_url=f'https://podcast.rthk.hk/podcast/upload_photo/item_photo/170x170_{programme_info.pid}.jpg',
        languages=['en'] if programme_info.language == '英文' else ['zh-HK']
    )
