import grpc

from ydb.public.api.client.yc_private.servicecontrol import access_service_pb2_grpc
from ydb.public.api.client.yc_private.servicecontrol.access_service_pb2 import \
    AuthenticateResponse, AuthorizeResponse, Subject


class AccessServiceMockState(object):
    def __init__(self):
        self._authenticate_subject = None
        self._authorize_subject = None

    @staticmethod
    def _check_anonymous_account(anonymous_account):
        if anonymous_account is not None and anonymous_account != dict():
            raise ValueError("anonymous_account must be either null or {}")

    @staticmethod
    def _check_user_account(user_account):
        if not isinstance(user_account, dict) or set(user_account.keys()) != {"id"}:
            raise ValueError('user_account must be {"id": "..."}')

    @staticmethod
    def _check_service_account(service_account):
        if not isinstance(service_account, dict) or set(service_account.keys()) != {"id", "folder_id"}:
            raise ValueError('service_account must be {"id": "...", "folder_id": "..."}')

    @staticmethod
    def _check_subject(subject):
        if subject is not None and not isinstance(subject, Subject):
            if not isinstance(subject, dict) or len(subject.keys()) != 1:
                raise ValueError('subject must be either {"anonymous_account": ...}, {"user_account": ...} or {"service_account": ...}')

            if "anonymous_account" in subject:
                AccessServiceMockState._check_anonymous_account(subject["anonymous_account"])
            elif "user_account" in subject:
                AccessServiceMockState._check_user_account(subject["user_account"])
            elif "service_account" in subject:
                AccessServiceMockState._check_service_account(subject["service_account"])
            else:
                raise ValueError('subject must be either {"anonymous_account": ...}, {"user_account": ...} or {"service_account": ...}')

    @property
    def authenticate_subject(self):
        return self._authenticate_subject

    @authenticate_subject.setter
    def authenticate_subject(self, value):
        AccessServiceMockState._check_subject(value)
        self._authenticate_subject = value

    @property
    def authorize_subject(self):
        return self._authorize_subject

    @authorize_subject.setter
    def authorize_subject(self, value):
        AccessServiceMockState._check_subject(value)
        self._authorize_subject = value


class AccessServiceMockServicer(access_service_pb2_grpc.AccessServiceServicer):
    def __init__(self, state):
        self._state = state

    @staticmethod
    def _copy_subject(src, dst):
        if "anonymous_account" in src:
            dst.anonymous_account.SetInParent()
        elif "user_account" in src:
            dst.user_account.id = src["user_account"]["id"]
        elif "service_account" in src:
            dst.service_account.id = src["service_account"]["id"]
            dst.service_account.folder_id = src["service_account"]["folder_id"]

    def Authenticate(self, request, context):
        subject = self._state.authenticate_subject
        if subject is None:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "The provided credentials are invalid or may have expired")

        response = AuthenticateResponse()
        self._copy_subject(subject, response.subject)

        return response

    def Authorize(self, request, context):
        subject = self._state.authorize_subject
        if subject is None:
            context.abort(grpc.StatusCode.PERMISSION_DENIED, "You do not currenly have permissions to access the resource")

        response = AuthorizeResponse()
        self._copy_subject(subject, response.subject)

        return response
