import argparse
import logging
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
    parser.add_argument('-d', '--debug', default=False, action='store_true', help='Debug mode')
    subparsers = parser.add_subparsers(required=True, dest='subcommand')

    from scripts import create_odysee_channel, download_podcast, list_odysee_videos, list_podcast_programmes, \
        upload_to_internet_archive, \
        upload_to_odysee, \
        youtube_json_to_csv
    create_odysee_channel.configure(
        subparsers.add_parser('create-odysee-channel', help='Create Odysee channel')
    )
    download_podcast.configure(
        subparsers.add_parser('download-podcast', help='Download podcast files')
    )
    list_odysee_videos.configure(
        subparsers.add_parser('list-odysee-videos', help='List Odysee videos')
    )
    list_podcast_programmes.configure(
        subparsers.add_parser('list-podcast-programmes', help='List podcast programmes')
    )
    upload_to_internet_archive.configure(
        subparsers.add_parser('upload-to-internet-archive', help='Upload videos to archive.org')
    )
    upload_to_odysee.configure(
        subparsers.add_parser('upload-to-odysee', help='Upload videos to Odysee')
    )
    youtube_json_to_csv.configure(
        subparsers.add_parser('youtube-json-to-csv', help='Convert youtube metadata JSON to csv')
    )

    args = parser.parse_args()
    _configure_logging(debug_mode=args.debug)

    if args.subcommand == 'create-odysee-channel':
        return create_odysee_channel.parse_args(args)
    elif args.subcommand == 'download-podcast':
        return download_podcast.parse_args(args)
    elif args.subcommand == 'list-odysee-videos':
        return list_odysee_videos.parse_args(args)
    elif args.subcommand == 'list-podcast-programmes':
        return list_podcast_programmes.parse_args(args)
    elif args.subcommand == 'upload-to-internet-archive':
        return upload_to_internet_archive.parse_args(args)
    elif args.subcommand == 'upload-to-odysee':
        return upload_to_odysee.parse_args(args)
    elif args.subcommand == 'youtube-json-to-csv':
        return youtube_json_to_csv.parse_args(args)
    raise ValueError(f'Unsupported command: {args.subcommand}')


def _configure_logging(debug_mode: bool):
    if debug_mode:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)
