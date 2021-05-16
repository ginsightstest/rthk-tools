## Usage

### Get podcast list

```
poetry run python3 main.py \
  list-podcast-programmes \
  --csv-out <path for writing output csv> \
  [--incremental] \
  [--pid <pid> ...]
```

### Download podcast

```
poetry run python3 main.py \
  download-podcast \
  --out-dir <directory path for writing videos>
  --csv-in <path to podcast list>
  [--pid <pid> ...] \
  [--year <year> ...]
```

### Convert youtube json to csv

```
poetry run python3 main.py \
  youtube-json-to-csv \
  --youtube-json-dir <path to directory with *.json files> \
  --csv-out <path for writing output csv>
```
