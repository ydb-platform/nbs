import logging

import yatest.common as common

logger = logging.getLogger(__name__)


class KikimrClient:
    def __init__(self, binary_path, port):
        self.__binary_path = binary_path
        self.__port = port
        self.__available_media_kinds = ['hdd', 'ssd']

    def get_ss_user_attrs(self):
        cmd = [
            self.__binary_path,
            '-s',
            f'localhost:{self.__port}',
            'db',
            'schema',
            'user-attribute',
            'get',
            '/Root/nfs',
        ]

        logger.info("getting user-attrs: " + " ".join(cmd))
        return common.execute(cmd).stdout

    def set_ss_filestore_limit(self, media_kind, value):
        if media_kind not in self.__available_media_kinds:
            logger.warning(f'Unknown media kind: {media_kind}. You can use one of: {self.__available_media_kinds}')
            return None

        cmd = [
            self.__binary_path,
            '-s',
            f'localhost:{self.__port}',
            'db',
            'schema',
            'user-attribute',
            'set',
            '/Root/nfs',
            f'__filestore_space_limit_{media_kind}={value}',
        ]

        logger.info("updating user-attrs: " + " ".join(cmd))
        return common.execute(cmd).stdout
