import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import date
from typing import List, NamedTuple, Optional

import pandas as pd

from model.odysee.publish import OdyseeClaimSearchApiRequest
from scripts.args import Args
from uploader.odysee_uploader import OdyseeUploader
from util.dates import ymd_to_date
from util.lists import flatten
from util.paths import to_abs_path


@dataclass
class ListOdyseeVideosArgs(Args):
    channel_id: str
    csv_out: str


class VideoInfo(NamedTuple):
    dt: Optional[date]
    title: str
    description: str
    url: str


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--channel-id', required=True, help='Odysee channel id')
    parser.add_argument('--csv-out', required=True, help='Path for output csv file')


def parse_args(raw_args: argparse.Namespace) -> ListOdyseeVideosArgs:
    channel_id = raw_args.channel_id
    csv_out = raw_args.csv_out

    return ListOdyseeVideosArgs(
        channel_id=channel_id,
        csv_out=to_abs_path(csv_out)
    )


def run(args: ListOdyseeVideosArgs):
    asyncio.run(
        _list_odysee_videos(
            args
        )
    )


async def _list_odysee_videos(args: ListOdyseeVideosArgs):
    claim_search_results = await _perform_claim_search(channel_id=args.channel_id)

    def _to_video_info(claim_search_result: dict) -> VideoInfo:
        raw_title = claim_search_result['value']['title']
        title = raw_title.removeprefix('Podcast One: ').rsplit('|', 1)[0].strip()
        if '|' in raw_title:
            dt = ymd_to_date(raw_title.rsplit('|', 1)[1].strip())
        else:
            dt = None
        url = claim_search_result['permanent_url'].replace('lbry://', 'https://odysee.com/')

        return VideoInfo(
            dt=dt,
            title=title,
            description=claim_search_result['value']['description'],
            url=url
        )

    video_infos = list(map(_to_video_info, claim_search_results))

    _write_to_csv(video_infos, csv_out=args.csv_out)
    logging.info(f"Wrote CSV file to: {args.csv_out}")


async def _perform_claim_search(channel_id: str) -> List[dict]:
    odysee_uploader = OdyseeUploader()

    async def _get_total_pages() -> int:
        resp = await odysee_uploader.search(OdyseeClaimSearchApiRequest(
            channel_ids=[channel_id],
            page_size=50
        ))
        return resp['total_pages']

    async def _get_results_for_page(page: int) -> List[dict]:
        resp = await odysee_uploader.search(OdyseeClaimSearchApiRequest(
            channel_ids=[channel_id],
            page_size=50,
            page=page
        ))
        return resp['items']

    total_pages = await _get_total_pages()
    nested_items = await asyncio.gather(*map(_get_results_for_page, range(1, total_pages + 1)))
    return flatten(nested_items)


def _write_to_csv(video_infos: List[VideoInfo], csv_out: str):
    frame = pd.DataFrame.from_records([info._asdict() for info in video_infos]) \
        .sort_values(by=["dt", "title"]) \
        .rename(columns={'dt': 'date'})
    frame.to_csv(csv_out, index=False)
