import logging

import internetarchive

from model.internetarchive.upload import InternetArchiveUploadApiRequest


class InternetArchiveUploader:
    def exists(self, identifier: str) -> bool:
        return internetarchive.search_items(f'identifier:{identifier}').num_found > 0

    def upload(self, publish_request: InternetArchiveUploadApiRequest):
        if self.exists(publish_request.identifier):
            logging.warning(f'Already exists on archive.org: {publish_request.file_path}, not overwriting!')
            return
        metadata = {k: v for k, v in publish_request._asdict().items() if v is not None}
        resp = internetarchive.upload(
            publish_request.identifier,
            files=[publish_request.file_path],
            metadata=metadata)
        if resp[0].status_code != 200:
            raise Exception(
                f'Failed to upload to archive.org: {publish_request.file_path}. Status code: {resp[0].status_code} Cause: {resp[0].text}')
        logging.info(f'Successfully uploaded {publish_request.file_path} to archive.org.')
