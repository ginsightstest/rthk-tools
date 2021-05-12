import argparse
import sys

from scripts import youtube_json_to_csv


class Args:
    pass


class Parser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


def parse_args() -> Args:
    parser = Parser()
    subparsers = parser.add_subparsers(required=True, dest='subcommand')

    youtube_json_to_csv.configure(
        subparsers.add_parser('youtube-json-to-csv', help='Convert youtube metadata JSON to csv'))

    args = parser.parse_args()

    if args.subcommand == 'youtube-json-to-csv':
        return youtube_json_to_csv.parse_args(args)
    raise ValueError(f'Unsupported command: {args.subcommand}')
