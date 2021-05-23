## Usage

### Create Odysee channel

```
poetry run python3 main.py \
  create-odysee-channel \
  --pid <pid> \
  [--bid <bid>]
```

### List Odysee videos

```
poetry run python3 main.py \
  list-odysee-videos \
  --channel-id <Odysee channel id>
  --csv-out <path for writing output csv>
```

### Get podcast list

```
poetry run python3 main.py \
  list-podcast-programmes \
  --csv-out <path for writing output csv> \
  [--incremental] \
  [--lang {zh-CN,en-US} ...]
  [--pid <pid> ...]
```

### Download podcast

```
poetry run python3 main.py \
  download-podcast \
  --out-dir <directory path for writing videos>
  --csv-in <path to podcast list>
  [--pid <pid> ...] \
  ([--eid <eid> ...] | [--year <year> ...])
```

### Upload to archive.org

```
poetry run python3 main.py \
  upload-to-internet-archive \
  --upload-dir <directory containing videos to upload> \
  --csv-in <path to podcast list> \
  [--with-date]
```

### Upload to Odysee

```
poetry run python3 main.py \
  upload-to-odysee \
  --upload-dir <directory containing videos to upload> \
  --csv-in <path to podcast list> \
  --channel-id <odysee channel id> \
  [--bid <bid>] \
  [--with-date]
```

### Convert youtube json to csv

```
poetry run python3 main.py \
  youtube-json-to-csv \
  --youtube-json-dir <path to directory with *.json files> \
  --csv-out <path for writing output csv>
```
