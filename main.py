import argparse
import glob
import logging
import os
from typing import Dict, Iterable, List, NamedTuple, Set

import ujson

from model.record import Record
from parser.programme_name_parser import ProgrammeNameParser
from parser.youtube.youtube_json_parser import YoutubeJsonParser
from writer.records_csv_writer import RecordsCsvWriter

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


class Args(NamedTuple):
    youtube_json_dir: str
    csv_out: str


def parse_args() -> Args:
    parser = argparse.ArgumentParser()
    youtube_json_dir_arg = parser.add_argument('--youtube-json-dir')
    parser.add_argument('--csv-out')

    args = parser.parse_args()
    youtube_json_dir, csv_out = args.youtube_json_dir, args.csv_out

    if not os.path.isdir(youtube_json_dir):
        raise argparse.ArgumentError(youtube_json_dir_arg, "Directory does not exist!")

    return Args(
        youtube_json_dir=args.youtube_json_dir,
        csv_out=args.csv_out
    )


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


def main():
    args = parse_args()

    raw_youtube_jsons = load_raw_youtube_json_files(args.youtube_json_dir)
    programme_names_with_counts = collect_programme_names_with_counts(raw_youtube_jsons)
    records = parse_records_from_raw_youtube_jsons(
        raw_youtube_jsons,
        programme_names=set(programme_names_with_counts.keys()))
    RecordsCsvWriter(records).write_to_csv(args.csv_out)


if __name__ == "__main__":
    main()
