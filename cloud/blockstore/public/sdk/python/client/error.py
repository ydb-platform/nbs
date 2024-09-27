import functools
import grpc

from .error_codes import EResult, EFacility, status_from_code, \
    format_error_code, make_grpc_error, facility_from_code

from .error_codes import succeeded as error_codes_succeeded
from .error_codes import failed as error_codes_failed


def _handle_errors(f):
    @functools.wraps(f)
    def wrapper(*args, **kw):
        try:
            return f(*args, **kw)
        except ClientError:
            raise
        except grpc.RpcError as e:
            raise ClientError.from_grpc_error(e)
        except Exception as e:
            raise ClientError(EResult.E_FAIL.value, str(e))

    return wrapper


class ClientError(RuntimeError):

    def __init__(self, code: int = EResult.S_OK.value, message : str = ""):
        self.code = code
        self.message = message

    @property
    def succeeded(self) -> int:
        return error_codes_succeeded(self.code)

    @property
    def failed(self) -> int:
        return error_codes_failed(self.code)

    @property
    def facility(self) -> int:
        return facility_from_code(self.code)

    @property
    def status(self) -> int:
        return status_from_code(self.code)

    @staticmethod
    def from_grpc_error(rpc_error: grpc.RpcError):
        status = rpc_error.code()
        code = make_grpc_error(status.value[0])
        return ClientError(code, rpc_error.details())

    @property
    def is_retriable(self) -> bool:
        # special error code for retries
        # NOTE: do not add E_TRY_AGAIN here - it is not retriable for a reason
        if self.code in [
            EResult.E_REJECTED.value,
            EResult.E_TIMEOUT.value,
            EResult.E_THROTTLED.value,
            EResult.E_OUT_OF_SPACE.value,
        ]:
            return True

        facility = self.facility

        if facility == EFacility.FACILITY_GRPC.value:
            if self.code == EResult.E_GRPC_UNIMPLEMENTED.value:
                return False
            # network errors should be retriable
            return True

        if facility == EFacility.FACILITY_SYSTEM.value:
            # system errors should be retriable
            return True

        if facility == EFacility.FACILITY_KIKIMR.value and self.status in [
            1,  # NKikimrProto::ERROR
            3,  # NKikimrProto::TIMEOUT
            4,  # NKikimrProto::RACE
            6,  # NKikimrProto::BLOCKED
            7,  # NKikimrProto::NOTREADY
            12,  # NKikimrProto::DEADLINE
            20,  # NKikimrProto::NOT_YET
        ]:
            return True

        if facility == EFacility.FACILITY_SCHEMESHARD.value and self.status in [
            13,  # NKikimrScheme::StatusNotAvailable
            8,   # NKikimrScheme::StatusMultipleModifications
        ]:
            return True

        if facility == EFacility.FACILITY_TXPROXY.value and self.status in [
            16,  # NKikimr::NTxProxy::TResultStatus::ProxyNotReady
            20,  # NKikimr::NTxProxy::TResultStatus::ProxyShardNotAvailable
            21,  # NKikimr::NTxProxy::TResultStatus::ProxyShardTryLater
            22,  # NKikimr::NTxProxy::TResultStatus::ProxyShardOverloaded
            51,  # NKikimr::NTxProxy::TResultStatus::ExecTimeout:
            55,  # NKikimr::NTxProxy::TResultStatus::ExecResultUnavailable:
        ]:
            return True

        # any other errors should not be retried automatically
        return False

    def __str__(self):
        if self.message:
            return format_error_code(self.code) + " " + self.message
        else:
            return format_error_code(self.code)


def client_error_from_response(response) -> ClientError:
    return ClientError(response.Error.Code, response.Error.Message)
