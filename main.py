import logging

from scripts import download_podcast, list_podcast_programmes, youtube_json_to_csv
from scripts.args import parse_args
from scripts.download_podcast import DownloadPodcastArgs
from scripts.list_podcast_programmes import ListPodcastProgrammesArgs
from scripts.youtube_json_to_csv import YoutubeToJsonArgs

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def main():
    args = parse_args()

    if isinstance(args, DownloadPodcastArgs):
        download_podcast.run(args)

    if isinstance(args, ListPodcastProgrammesArgs):
        list_podcast_programmes.run(args)

    if isinstance(args, YoutubeToJsonArgs):
        youtube_json_to_csv.run(args)

    logging.info('Completed successfully.')


if __name__ == "__main__":
    main()
