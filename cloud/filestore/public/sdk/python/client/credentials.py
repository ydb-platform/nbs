import grpc
import os


_MAX_CERT_FILE_SIZE = 8 * 1024 * 1024


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
            credentials = grpc.composite_channel_credentials(
                credentials,
                grpc.access_token_call_credentials(self.auth_token),
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
