import logging

import internetarchive
import requests

from model.internetarchive.upload import InternetArchiveUploadApiRequest


class InternetArchiveUploader:
    def exists(self, identifier: str) -> bool:
        return internetarchive.search_items(f'identifier:{identifier}').num_found > 0

    def upload(self, publish_request: InternetArchiveUploadApiRequest):
        if self.exists(publish_request.identifier):
            logging.warning(f'Already exists on archive.org: {publish_request.file_path}, not overwriting!')
            return
        metadata = {k: v for k, v in publish_request._asdict().items() if v is not None}

        def _upload_with_identifier(identifier: str):
            internetarchive.upload(
                identifier,
                files=[publish_request.file_path],
                metadata=metadata)
            logging.info(f'Successfully uploaded {publish_request.file_path} to archive.org.')

        try:
            _upload_with_identifier(publish_request.identifier)
        except requests.RequestException:
            i = 1
            while True:
                try:
                    retry_identifier = f'{publish_request.identifier}_{i}'
                    logging.warning(
                        f'Could not upload with identifier {publish_request.identifier}. Retrying with: {retry_identifier}')
                    _upload_with_identifier(retry_identifier)
                    break
                except requests.RequestException:
                    i += 1
