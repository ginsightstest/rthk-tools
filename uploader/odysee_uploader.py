import logging

import aiohttp
import ujson

from model.odysee.publish import OdyseeChannelCreateApiRequest, OdyseePublishApiRequest


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

    async def upload(self, publish_request: OdyseePublishApiRequest):
        data = ujson.dumps({
            'method': 'publish',
            'params': {k: v for k, v in publish_request._asdict().items() if v is not None}
        })
        async with aiohttp.ClientSession() as client:
            async with client.post('http://localhost:5279',
                                   data=data) as resp:
                j = await resp.json()
                if 'error' in j:
                    stacktrace = '\n'.join(j['error']['data']['traceback'])
                    logging.fatal(
                        f'Failed to upload {publish_request.file_path} to Odysee. Cause: {stacktrace}')
                    exit(-1)
                else:
                    logging.info(
                        f'Successfully uploaded {publish_request.file_path} to Odysee. Fee: {j["result"]["total_fee"]} Transaction id: {j["result"]["txid"]}')
