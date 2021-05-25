import argparse
import asyncio
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import List, NamedTuple, Optional

from csv_reader_writer.backup_table_writer import BackupTableWriter
from csv_reader_writer.episodes_csv_reader import EpisodesCsvReader
from model.backup_table import BackupTable, BackupTableRow
from model.odysee.publish import OdyseeClaimSearchApiRequest
from model.podcast.episode import Episode
from scripts.args import Args
from uploader.odysee_uploader import OdyseeUploader
from util.dates import ymd_to_date
from util.paths import to_abs_path


@dataclass
class ListOdyseeVideosArgs(Args):
    channel_id: str
    csv_in: str
    csv_out: str


class OdyseeVideoInfo(NamedTuple):
    dt: Optional[date]
    title: str
    description: str
    url: str


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--channel-id', required=True, help='Odysee channel id')
    parser.add_argument('--csv-in', required=True, help='Path to podcast list csv')
    parser.add_argument('--csv-out', required=True, help='Path for output csv file')


def parse_args(raw_args: argparse.Namespace) -> ListOdyseeVideosArgs:
    channel_id = raw_args.channel_id
    csv_in = raw_args.csv_in
    csv_out = raw_args.csv_out

    return ListOdyseeVideosArgs(
        channel_id=channel_id,
        csv_in=to_abs_path(csv_in),
        csv_out=to_abs_path(csv_out)
    )


def run(args: ListOdyseeVideosArgs):
    asyncio.run(
        _list_odysee_videos(
            args
        )
    )


async def _list_odysee_videos(args: ListOdyseeVideosArgs):
    episodes = EpisodesCsvReader().read_to_episodes(args.csv_in)
    odysee_video_infos = await _list_odysee_video_infos(channel_id=args.channel_id)
    backup_table = _merge_into_backup_table(odysee_video_infos, episodes)
    BackupTableWriter(backup_table).write_to_csv(args.csv_out)


async def _list_odysee_video_infos(channel_id: str) -> List[OdyseeVideoInfo]:
    odysee_uploader = OdyseeUploader()

    async def _get_total_pages() -> int:
        resp = await odysee_uploader.search(OdyseeClaimSearchApiRequest(
            channel_ids=[channel_id],
            page_size=50
        ))
        return resp['total_pages']

    async def _get_results_for_page(page: int) -> List[OdyseeVideoInfo]:
        resp = await odysee_uploader.search(OdyseeClaimSearchApiRequest(
            channel_ids=[channel_id],
            page_size=50,
            page=page
        ))
        return resp['items']

    def _to_odysee_video_info(claim_search_result: dict) -> OdyseeVideoInfo:
        raw_title = claim_search_result['value']['title']
        title = raw_title.rsplit('|', 1)[0].strip()
        if '|' in raw_title:
            dt = ymd_to_date(raw_title.rsplit('|', 1)[1].strip())
        else:
            dt = None
        url = claim_search_result['permanent_url'].replace('lbry://', 'https://odysee.com/')

        return OdyseeVideoInfo(
            dt=dt,
            title=title,
            description=claim_search_result['value'].get('description', ''),
            url=url
        )

    total_pages = await _get_total_pages()
    nested_claim_search_results = await asyncio.gather(*map(_get_results_for_page, range(1, total_pages + 1)))
    return [_to_odysee_video_info(result)
            for results in nested_claim_search_results
            for result in results]


def _merge_into_backup_table(odysee_video_infos: List[OdyseeVideoInfo], episodes: List[Episode]) -> BackupTable:
    episodes_by_eid = {e.eid: e for e in episodes}
    eids_by_title = defaultdict(set)
    eids_by_description = defaultdict(set)
    eids_by_date = defaultdict(set)
    for episode in episodes:
        eids_by_title[episode.og_title.strip()].add(episode.eid)
        if episode.og_description:
            eids_by_description[re.sub(r'\s+', '', episode.og_description)].add(episode.eid)
        eids_by_date[episode.episode_date].add(episode.eid)

    backup_table = []
    for odysee_video_info in odysee_video_infos:
        potential_eids_by_title = eids_by_title[odysee_video_info.title.strip()]
        potential_eids_by_description = None
        if odysee_video_info.description:
            potential_eids_by_description = eids_by_description[re.sub(r'\s+', '', odysee_video_info.description)]
        potential_eids_by_date = eids_by_date[odysee_video_info.dt]

        eids_set = potential_eids_by_title
        if len(eids_set) > 1 and potential_eids_by_description:
            eids_set &= potential_eids_by_description
        if len(eids_set) > 1:
            eids_set &= potential_eids_by_date

        if not len(eids_set) == 1:
            raise ValueError(f'Failed to find episode for odysee video info: {odysee_video_info}')
        eid = next(iter(eids_set))
        episode = episodes_by_eid[eid]

        row = BackupTableRow(
            programme=episode.programme_title,
            dt=episode.episode_date,
            title=episode.episode_title,
            content_description=episode.og_description,
            youtube_link=f'https://podcast.rthk.hk/podcast/item.php?pid={episode.pid}&eid={episode.eid}',
            backup_link=odysee_video_info.url
        )
        backup_table.append(row)

    return backup_table
