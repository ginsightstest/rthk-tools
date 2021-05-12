import logging

from scripts import youtube_json_to_csv
from scripts.args import parse_args
from scripts.youtube_json_to_csv import YoutubeToJsonArgs

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def main():
    args = parse_args()

    if isinstance(args, YoutubeToJsonArgs):
        youtube_json_to_csv.run(args)


if __name__ == "__main__":
    main()
