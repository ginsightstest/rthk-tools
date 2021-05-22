import asyncio
import logging
import os
from typing import Optional

from crawler.podcast.client import get_resumable


class Mp4Downloader:

    def __init__(self, sem: asyncio.Semaphore):
        self._sem = sem

    async def save_download(self, mp4_url: str, out_path: str, tqdm_local_position: Optional[int] = None):
        if os.path.exists(out_path):
            logging.info(f'File already downloaded: {out_path}')
            return

        basename, ext = os.path.splitext(out_path)
        tmp_ext = f'{ext}.tmp'
        tmp_out_path = basename + tmp_ext
        await get_resumable(mp4_url, write_to_file=tmp_out_path, sem=self._sem, tqdm_local_position=tqdm_local_position)
        os.rename(src=tmp_out_path, dst=out_path)
