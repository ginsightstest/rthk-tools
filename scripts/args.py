import argparse
import sys


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

    from scripts import download_podcast, list_podcast_programmes, upload_to_odysee, youtube_json_to_csv
    download_podcast.configure(
        subparsers.add_parser('download-podcast', help='Download podcast files')
    )
    list_podcast_programmes.configure(
        subparsers.add_parser('list-podcast-programmes', help='List podcast programmes')
    )
    upload_to_odysee.configure(
        subparsers.add_parser('upload-to-odysee', help='Upload videos to Odysee')
    )
    youtube_json_to_csv.configure(
        subparsers.add_parser('youtube-json-to-csv', help='Convert youtube metadata JSON to csv')
    )

    args = parser.parse_args()

    if args.subcommand == 'download-podcast':
        return download_podcast.parse_args(args)
    elif args.subcommand == 'list-podcast-programmes':
        return list_podcast_programmes.parse_args(args)
    elif args.subcommand == 'upload-to-odysee':
        return upload_to_odysee.parse_args(args)
    elif args.subcommand == 'youtube-json-to-csv':
        return youtube_json_to_csv.parse_args(args)
    raise ValueError(f'Unsupported command: {args.subcommand}')
