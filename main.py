import logging

from scripts import create_odysee_channel, download_podcast, list_podcast_programmes, upload_to_internet_archive, \
    upload_to_odysee, \
    youtube_json_to_csv
from scripts.args import parse_args
from scripts.create_odysee_channel import CreateOdyseeChannelArgs
from scripts.download_podcast import DownloadPodcastArgs
from scripts.list_podcast_programmes import ListPodcastProgrammesArgs
from scripts.upload_to_internet_archive import UploadToInternetArchiveArgs
from scripts.upload_to_odysee import UploadToOdyseeArgs
from scripts.youtube_json_to_csv import YoutubeToJsonArgs

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def main():
    args = parse_args()

    if isinstance(args, CreateOdyseeChannelArgs):
        create_odysee_channel.run(args)

    if isinstance(args, DownloadPodcastArgs):
        download_podcast.run(args)

    if isinstance(args, ListPodcastProgrammesArgs):
        list_podcast_programmes.run(args)

    if isinstance(args, UploadToInternetArchiveArgs):
        upload_to_internet_archive.run(args)

    if isinstance(args, UploadToOdyseeArgs):
        upload_to_odysee.run(args)

    if isinstance(args, YoutubeToJsonArgs):
        youtube_json_to_csv.run(args)

    logging.info('Completed successfully.')


if __name__ == "__main__":
    main()
