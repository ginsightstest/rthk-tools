import asyncio
import logging
import os
import re
from typing import Optional

import aiofiles
import aiohttp
import tqdm
from aiohttp import ClientError


async def get(url: str, sem: asyncio.Semaphore):
    """
    >>> '<!doctype html>' in asyncio.run(get('http://google.com', sem=asyncio.Semaphore()))
    True
    """
    async with sem:
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as resp:
                text = await resp.text()
    return text


async def get_content_length(url: str, sem: asyncio.Semaphore, num_retries: int = 0, timeout: int = 300) -> Optional[
    int]:
    while num_retries > -1:
        try:
            async with sem:
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as client:
                    async with client.head(url) as resp:
                        content_length = resp.headers.get('Content-Length')
                        logging.debug(f'Got content length {content_length} for url: {url}')
                        return int(content_length) if content_length else None
        except asyncio.TimeoutError:
            num_retries -= 1
            logging.warning(f'Retrying get content_length: {url}. Remaining retries: {num_retries}')


class NotResumableError(Exception):
    pass


async def get_resumable(url: str,
                        write_to_file: str,
                        sem: asyncio.Semaphore,
                        progress_bar: Optional[tqdm.tqdm] = None,
                        tqdm_local_position: Optional[int] = None):
    async with sem:
        async with aiohttp.ClientSession() as client:
            async with client.head(url) as resp:
                accept_ranges = resp.headers.get('Accept-Ranges')
                content_length = resp.headers.get('Content-Length')
                if not accept_ranges or not 'bytes' in accept_ranges or not content_length:
                    raise NotResumableError(f'URL does not support resume: {url}')

            local_progress_bar = progress_bar
            if not progress_bar:
                local_progress_bar = tqdm.tqdm(total=int(content_length) / 1024,
                                               unit='KB',
                                               position=tqdm_local_position)

            if not os.path.exists(write_to_file):
                async with aiofiles.open(write_to_file, mode='w'):
                    pass

            while True:
                try:
                    async with aiofiles.open(write_to_file, mode='r+b') as f:
                        async with client.get(url, headers={
                            'Range': f'bytes={os.path.getsize(write_to_file)}-{content_length}'
                        }) as resp:
                            if resp.status == 416:
                                logging.debug(f'Download already complete for url: {url}')
                                local_progress_bar.update(os.path.getsize(write_to_file) / 1024)
                            else:
                                start_bytes = int(re.search(r'bytes (\d+)-\d+', resp.headers['Content-Range']).group(1))
                                logging.debug(f'Resuming download from byte position {start_bytes} for: {url}')
                                local_progress_bar.update(start_bytes / 1024)
                                await f.seek(start_bytes)

                                async for chunk in resp.content.iter_chunked(1024):
                                    await f.write(chunk)
                                    local_progress_bar.update(1)
                    break
                except ClientError:
                    logging.warning(f'Will retry resumable download: {url}', exc_info=True)

            if not progress_bar:
                local_progress_bar.close()


async def get_bytes(url: str,
                    sem: asyncio.Semaphore,
                    progress_bar: Optional[tqdm.tqdm] = None,
                    tqdm_local_position: Optional[int] = None) -> bytes:
    async with sem:
        async with aiohttp.ClientSession() as client:
            local_progress_bar = progress_bar
            if not progress_bar:
                local_progress_bar = tqdm.tqdm(unit='KB',
                                               position=tqdm_local_position)
            while True:
                try:
                    async with aiofiles.tempfile.TemporaryFile(mode='w+b') as f:
                        async with client.get(url) as resp:
                            async for chunk in resp.content.iter_chunked(1024):
                                await f.write(chunk)
                                local_progress_bar.update(1)

                        await f.seek(0)
                        raw_bytes = await f.read()
                    break
                except ClientError:
                    logging.warning(f'Will retry non-resumable download: {url}', exc_info=True)

            if not progress_bar:
                local_progress_bar.close()

            return raw_bytes


if __name__ == "__main__":
    import doctest

    doctest.testmod()
