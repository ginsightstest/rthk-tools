import argparse
from collections import defaultdict
from dataclasses import dataclass
from typing import List

from csv_reader_writer.backup_table_reader import BackupTableReader
from model.backup_table import BackupTable, BackupTableRow
from scripts.args import Args
from util.dates import date_to_ymd
from util.paths import to_abs_path


@dataclass
class CreateOdyseeReadmeArgs(Args):
    backup_table_csv_in: str
    md_out: str


def configure(parser: argparse.ArgumentParser):
    parser.add_argument('--backup-table-csv-in', required=True, help='Path to backup table csv')
    parser.add_argument('--md-out', required=True, help='Path for output markdown file')
    parser.add_argument('--with-date', default=False, action='store_true', help='Whether to add date to title')


def parse_args(raw_args: argparse.Namespace) -> CreateOdyseeReadmeArgs:
    backup_table_csv_in = raw_args.backup_table_csv_in
    md_out = raw_args.md_out

    return CreateOdyseeReadmeArgs(
        backup_table_csv_in=to_abs_path(backup_table_csv_in),
        md_out=to_abs_path(md_out)
    )


def run(args: CreateOdyseeReadmeArgs):
    _create_odysee_readme(args)


def _create_odysee_readme(args: CreateOdyseeReadmeArgs):
    backup_table = BackupTableReader().read_to_backup_table(args.backup_table_csv_in)
    markdown_lines = _convert_to_readme(backup_table)
    with open(args.md_out, mode='w') as f:
        f.write('\n'.join(markdown_lines))


def _convert_to_readme(backup_table: BackupTable) -> List[str]:
    year_to_paragraph = defaultdict(list)

    def _to_markdown_row(row: BackupTableRow) -> str:
        row_date = date_to_ymd(row.dt)
        return f'| {row_date} | {row.title} | {row.backup_link} |'

    years = sorted(set([row.dt.year for row in backup_table]))

    for year in years:
        year_rows = [row for row in backup_table if row.dt.year == year]
        paragraph_lines = ['|播出日期|節目名稱|影片|',
                           '|---|---|---|'] \
                          + [_to_markdown_row(row) for row in year_rows]
        year_to_paragraph[year] = paragraph_lines

    lines = []
    for year in reversed(years):
        lines.append(f'# {year}')
        lines.extend(year_to_paragraph[year])
        lines.append('')

    return lines
