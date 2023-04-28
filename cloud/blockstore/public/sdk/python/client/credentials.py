import grpc
import os


_MAX_CERT_FILE_SIZE = 8 * 1024 * 1024
_AUTH_HEADER = "authorization"
_AUTH_METHOD = "Bearer"


class _AuthMetadataPlugin(grpc.AuthMetadataPlugin):

    def __init__(self, auth_token):
        super(grpc.AuthMetadataPlugin, self).__init__()
        self._auth_token = auth_token

    def __call__(self, context, callback):
        callback(((_AUTH_HEADER, _AUTH_METHOD + " " + self._auth_token),), None)


class ClientCredentials(object):

    def __init__(
            self,
            root_certs_file=None,
            cert_file=None,
            cert_private_key_file=None,
            auth_token=None):

        self.root_certs_file = root_certs_file
        self.cert_file = cert_file
        self.cert_private_key_file = cert_private_key_file
        self.auth_token = auth_token

    def get_ssl_channel_credentials(self):
        credentials = grpc.ssl_channel_credentials(
            root_certificates=self._read_file_optional(self.root_certs_file),
            private_key=self._read_file_optional(self.cert_private_key_file),
            certificate_chain=self._read_file_optional(self.cert_file),
        )

        if self.auth_token is not None:
            auth_metadata_plugin = _AuthMetadataPlugin(self.auth_token)
            credentials = grpc.composite_channel_credentials(
                credentials,
                grpc.metadata_call_credentials(auth_metadata_plugin),
            )

        return credentials

    @staticmethod
    def _read_file_optional(filename=None):
        if filename is None:
            return None

        st = os.stat(filename)
        if st.st_size > _MAX_CERT_FILE_SIZE:
            raise Exception("file too large")

        with open(filename, "rb") as f:
            return f.read()
