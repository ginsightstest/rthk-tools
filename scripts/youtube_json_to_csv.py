import argparse
import glob
import logging
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Set

import ujson

from model.record import Record
from parser.programme_name_parser import ProgrammeNameParser
from parser.youtube.youtube_json_parser import YoutubeJsonParser
from scripts.args import Args
from writer.records_csv_writer import RecordsCsvWriter


@dataclass
class YoutubeToJsonArgs(Args):
    youtube_json_dir: str
    csv_out: str


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--youtube-json-dir', required=True, help='Directory containing youtube *.json files')
    parser.add_argument('--csv-out', required=True, help='Path for output csv file')


def parse_args(raw_args: argparse.Namespace) -> YoutubeToJsonArgs:
    youtube_json_dir, csv_out = raw_args.youtube_json_dir, raw_args.csv_out
    if not os.path.isdir(youtube_json_dir):
        raise argparse.ArgumentError(None, f"--youtube-json-dir is not a directory: {youtube_json_dir}")
    return YoutubeToJsonArgs(
        youtube_json_dir=raw_args.youtube_json_dir,
        csv_out=raw_args.csv_out
    )


def run(args: YoutubeToJsonArgs):
    raw_youtube_jsons = load_raw_youtube_json_files(args.youtube_json_dir)
    programme_names_with_counts = collect_programme_names_with_counts(raw_youtube_jsons)
    records = parse_records_from_raw_youtube_jsons(
        raw_youtube_jsons,
        programme_names=set(programme_names_with_counts.keys()))
    RecordsCsvWriter(records).write_to_csv(args.csv_out)


def load_raw_youtube_json_files(youtube_json_dir: str) -> List[Dict]:
    paths = glob.glob(os.path.join(youtube_json_dir, "*.json"))
    raw_youtube_jsons = []
    progress, total = 0, len(paths)
    for path in paths:
        with open(path, 'r') as file:
            json = ujson.load(file)
        raw_youtube_jsons.append(json)

        # Because this takes a while, we print out progress bar...
        progress += 1
        if progress % 1000 == 0:
            logging.info(f"Loading raw youtube json files... {progress} out of {total}")
    return raw_youtube_jsons


def collect_programme_names_with_counts(raw_youtube_jsons: Iterable[Dict]) -> Dict[str, int]:
    titles = [json["fulltitle"] for json in raw_youtube_jsons]
    programme_names_with_counts = ProgrammeNameParser() \
        .collect_programme_names_with_counts(titles)
    logging.info(f"Collected {len(programme_names_with_counts)} programme names: {programme_names_with_counts}")
    return programme_names_with_counts


def parse_records_from_raw_youtube_jsons(raw_youtube_jsons: Iterable[Dict], programme_names: Set[str]) -> List[Record]:
    youtube_json_parser = YoutubeJsonParser(programme_names=programme_names)
    return [youtube_json_parser.parse_json(json) for json in raw_youtube_jsons]
