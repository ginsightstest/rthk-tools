import asyncio

from crawler.podcast.client import get_resumable


class Mp4Downloader:

    def __init__(self, sem: asyncio.Semaphore):
        self._sem = sem

    async def save_download(self, mp4_url: str, out_path: str):
        await get_resumable(mp4_url, write_to_file=out_path, sem=self._sem)
