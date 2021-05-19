import asyncio
import logging
import os
from typing import List

import aiofiles
import ffmpeg
import tqdm

from crawler.podcast import client


class M3U8Downloader:
    def __init__(self, sem: asyncio.Semaphore):
        self._sem = sem

    async def save_download(self, m3u8_url: str, out_path: str):
        if os.path.exists(out_path):
            logging.info(f'File already downloaded: {out_path}')
            return

        best_chunklist_url = await self._get_best_chunklist_url(m3u8_url)
        chunk_urls = await self._get_chunk_urls(best_chunklist_url)
        logging.debug(f'Got chunk urls: {chunk_urls}')
        progress_bar = await self._initialise_progress_bar(m3u8_url=m3u8_url, chunk_urls=chunk_urls)
        chunk_paths = await asyncio.gather(*[self._download_and_save_chunk(i, chunk_url, out_path, progress_bar)
                                             for i, chunk_url in enumerate(chunk_urls)])
        progress_bar.close()
        await self._merge_chunks_and_save_to_file(list(chunk_paths), out_path)

    async def _get_best_chunklist_url(self, m3u8_url: str) -> str:
        chunklist_urls = await self._parse_m3u8_contents(m3u8_url)
        # Assume last chunklist offers highest resolution
        return chunklist_urls[-1]

    async def _get_chunk_urls(self, chunklist_url: str):
        chunk_urls = await self._parse_m3u8_contents(chunklist_url)
        return chunk_urls

    async def _initialise_progress_bar(self, m3u8_url: str, chunk_urls: List[str]) -> tqdm.tqdm:
        content_lengths = await asyncio.gather(
            *[
                # HEAD request is cheap, no need mutex
                client.get_content_length(chunk_url, sem=asyncio.Semaphore())
                for chunk_url in chunk_urls
            ]
        )
        total_length = sum(content_lengths)
        progress_bar = tqdm.tqdm(total=int(total_length) / 1024,
                                 unit='KB',
                                 desc=f'Downloading {m3u8_url}')
        return progress_bar

    async def _download_and_save_chunk(self, chunk_num: int, chunk_url: str, out_path: str,
                                       progress_bar: tqdm.tqdm) -> str:
        basename, ext = os.path.splitext(out_path)
        chunk_ext = f'{ext}.chunk.{chunk_num}'
        chunk_out_path = basename + chunk_ext
        await client.get_resumable(chunk_url,
                                   write_to_file=chunk_out_path,
                                   sem=self._sem,
                                   progress_bar=progress_bar)
        return chunk_out_path

    async def _merge_chunks_and_save_to_file(self, chunk_paths: List[str], out_path: str):
        async with aiofiles.tempfile.NamedTemporaryFile(mode='wb') as tmp_file:
            for chunk_path in chunk_paths:
                async with aiofiles.open(chunk_path, mode='rb') as f:
                    chunk_bytes = await f.read()
                    await tmp_file.write(chunk_bytes)
            try:
                ffmpeg \
                    .input(tmp_file.name, fflags='+discardcorrupt') \
                    .output(out_path, vcodec='copy', acodec='copy') \
                    .run()
            except Exception as e:
                if os.path.exists(out_path):
                    os.remove(out_path)
                raise e
        for path in chunk_paths:
            os.remove(path)

    async def _parse_m3u8_contents(self, m3u8_url: str) -> List[str]:
        def _to_absolute_url(relative_url: str) -> str:
            if relative_url.startswith('http'):
                # already absolute, no resolution needed
                return relative_url
            return f'{m3u8_url[:m3u8_url.rfind("/")]}/{relative_url}'

        txt = await client.get(m3u8_url, sem=self._sem)
        return [_to_absolute_url(line)
                for line in txt.splitlines()
                if not line.startswith('#')]
