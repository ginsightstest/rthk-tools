import asyncio
import logging
import os
from typing import List

import aiofiles
import ffmpeg
import tqdm

from crawler.podcast import client
from crawler.podcast.client import NotResumableError


class M3U8Downloader:
    def __init__(self, sem: asyncio.Semaphore):
        self._sem = sem

    async def save_download(self, m3u8_url: str, out_path: str):
        if os.path.exists(out_path):
            logging.info(f'File already downloaded: {out_path}')
            return

        # Hack: replace stmw.rthk.hk with stmw3.rthk.hk because it appears most reliable
        m3u8_url = m3u8_url.replace('stmw.rthk.hk', 'stmw3.rthk.hk')

        best_chunklist_url = await self._get_best_chunklist_url(m3u8_url)
        chunk_urls = await self._get_chunk_urls(best_chunklist_url)
        logging.debug(f'Got chunk urls: {chunk_urls}')
        progress_bar = await self._initialise_progress_bar(chunk_urls=chunk_urls)
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

    async def _initialise_progress_bar(self, chunk_urls: List[str]) -> tqdm.tqdm:
        content_lengths = await asyncio.gather(
            *[
                client.get_content_length(chunk_url, sem=self._sem, num_retries=3, timeout=30)
                for chunk_url in chunk_urls
            ]
        )
        total_length = sum(content_lengths)
        progress_bar = tqdm.tqdm(total=int(total_length) / 1024, unit='KB')
        return progress_bar

    async def _download_and_save_chunk(self, chunk_num: int, chunk_url: str, out_path: str,
                                       progress_bar: tqdm.tqdm) -> str:
        basename, ext = os.path.splitext(out_path)
        chunk_ext = f'{ext}.chunk.{chunk_num}'
        chunk_out_path = basename + chunk_ext
        try:
            await client.get_resumable(chunk_url,
                                       write_to_file=chunk_out_path,
                                       sem=self._sem,
                                       progress_bar=progress_bar)
        except NotResumableError:
            logging.warning(f'Cannot resume download chunk: {chunk_url}', exc_info=True)
            if os.path.exists(chunk_out_path):
                logging.debug(f'Chunk already downloaded: {chunk_url}')
            else:
                logging.warning(f'Falling back to non-resumable download: {chunk_url}')
                raw_bytes = await client.get_bytes(chunk_url, sem=self._sem)
                async with aiofiles.open(chunk_out_path, mode='wb') as f:
                    await f.write(raw_bytes)

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
