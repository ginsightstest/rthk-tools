import asyncio
import logging

import aiohttp
import ujson

from model.odysee.publish import OdyseeChannelCreateApiRequest, OdyseeClaimSearchApiRequest, OdyseePublishApiRequest


class OdyseeUploader:
    async def create_channel(self, channel_create_request: OdyseeChannelCreateApiRequest):
        data = ujson.dumps({
            'method': 'channel_create',
            'params': {k: v for k, v in channel_create_request._asdict().items() if v is not None}
        })
        async with aiohttp.ClientSession() as client:
            async with client.post('http://localhost:5279',
                                   data=data) as resp:
                j = await resp.json()
                if 'error' in j:
                    stacktrace = '\n'.join(j['error']['data']['traceback'])
                    logging.fatal(
                        f'Failed to create channel {channel_create_request.name} on Odysee. Cause: {stacktrace}')
                    exit(-1)
                else:
                    logging.info(
                        f'Successfully created channel {channel_create_request.name} to Odysee. Fee: {j["result"]["total_fee"]} Transaction id: {j["result"]["txid"]}')

    async def search(self, claim_search_request: OdyseeClaimSearchApiRequest) -> dict:
        data = ujson.dumps({
            'method': 'claim_search',
            'params': {k: v for k, v in claim_search_request._asdict().items() if v is not None}
        })
        async with aiohttp.ClientSession() as client:
            async with client.post('http://localhost:5279',
                                   data=data) as resp:
                j = await resp.json()
                if 'error' in j:
                    stacktrace = '\n'.join(j['error']['data']['traceback'])
                    logging.fatal(
                        f'Failed to perform claim search on Odysee. Cause: {stacktrace}')
                    exit(-1)
                else:
                    return j['result']

    async def upload(self, publish_request: OdyseePublishApiRequest):
        data = ujson.dumps({
            'method': 'publish',
            'params': {k: v for k, v in publish_request._asdict().items() if v is not None}
        })
        while True:
            async with aiohttp.ClientSession() as client:
                async with client.post('http://localhost:5279',
                                       data=data) as resp:
                    j = await resp.json()
                    if 'error' in j:
                        stacktrace = '\n'.join(j['error']['data']['traceback'])
                        if 'too-long-mempool-chain' in stacktrace:
                            # Retry after a while
                            logging.warning(
                                f'Will retry upload {publish_request.file_path} after 1 min... Cause: {stacktrace}')
                            await asyncio.sleep(60)
                            continue
                        else:
                            logging.fatal(
                                f'Failed to upload {publish_request.file_path} to Odysee. Cause: {stacktrace}')
                            exit(-1)
                    else:
                        logging.info(
                            f'Successfully uploaded {publish_request.file_path} to Odysee. Fee: {j["result"]["total_fee"]} Transaction id: {j["result"]["txid"]}')
                        break
