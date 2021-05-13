## Usage

### Get RTHK podcast list

```sh
poetry run python3 main.py \
  list-podcast-programmes \
  --csv-out <path for writing output csv>
  [--pid <pid> ...]
```

### Convert youtube json to csv

```sh
poetry run python3 main.py \
  youtube-json-to-csv \
  --youtube-json-dir <path to directory with *.json files> \
  --csv-out <path for writing output csv>
```