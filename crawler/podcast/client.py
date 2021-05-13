import asyncio

import aiohttp


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


if __name__ == "__main__":
    import doctest

    doctest.testmod()
