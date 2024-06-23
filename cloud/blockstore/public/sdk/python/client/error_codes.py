import enum

##########################################################################
# We should combine errors from different facilities:
# * Generic errors
# * System errors (file system, network etc)
# * gRPC errors
# * KiKiMR errors
# * Application errors (storage, service etc)


class ESeverity(enum.Enum):
    SEVERITY_SUCCESS = 0
    SEVERITY_ERROR = 1


class EFacility(enum.Enum):
    FACILITY_NULL = 0
    FACILITY_SYSTEM = 1
    FACILITY_GRPC = 2
    FACILITY_KIKIMR = 3
    FACILITY_SCHEMESHARD = 4
    FACILITY_SERVICE = 5
    FACILITY_TXPROXY = 6


def succeeded(code: int) -> bool:
    return (code & 0x80000000 == 0)


def failed(code: int) -> bool:
    return (code & 0x80000000 != 0)


def facility_from_code(code: int) -> int:
    return (code & 0x7FFF0000) >> 16


def status_from_code(code: int) -> int:
    return (code & 0x0000FFFF)


def make_result_code(severity: int, facility: int, status: int) -> int:
    return ((severity & 0x00000001) << 31) | \
        ((facility & 0x00007FFF) << 16) | \
        (status & 0x0000FFFF)


def make_success(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_SUCCESS.value,
        EFacility.FACILITY_NULL.value,
        status)


def make_error(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_ERROR.value,
        EFacility.FACILITY_NULL.value,
        status)


def make_system_error(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_ERROR.value,
        EFacility.FACILITY_SYSTEM.value,
        status)


def make_grpc_error(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_ERROR.value,
        EFacility.FACILITY_GRPC.value,
        status)


def make_kikimr_error(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_ERROR.value,
        EFacility.FACILITY_KIKIMR.value,
        status)


def make_scheme_shard_error(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_ERROR.value,
        EFacility.FACILITY_SCHEMESHARD.value,
        status)


def make_service_error(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_ERROR.value,
        EFacility.FACILITY_SERVICE.value,
        status)


def make_txproxy_error(status: int) -> int:
    return make_result_code(
        ESeverity.SEVERITY_ERROR.value,
        EFacility.FACILITY_TXPROXY.value,
        status)

##########################################################################
# Well-known result codes


class EResult(enum.Enum):

    # success values
    S_OK = make_success(0)
    S_FALSE = make_success(1)
    S_ALREADY = make_success(2)

    # error values
    E_FAIL = make_error(0)
    E_ARGUMENT = make_error(1)
    E_REJECTED = make_error(2)
    # E_IO = make_error(3) # deprecated value
    E_INVALID_STATE = make_error(4)
    E_TIMEOUT = make_error(5)
    E_NOT_FOUND = make_error(6)
    E_UNAUTHORIZED = make_error(7)
    E_NOT_IMPLEMENTED = make_error(8)
    E_ABORTED = make_error(9)
    E_TRY_AGAIN = make_error(10)
    E_IO = make_error(11)
    E_CANCELLED = make_error(12)
    E_IO_SILENT = make_error(13)
    E_RETRY_TIMEOUT = make_error(14)
    E_PRECONDITION_FAILED = make_error(15)

    # grpc errors
    E_GRPC_CANCELLED = make_grpc_error(1)
    E_GRPC_UNKNOWN = make_grpc_error(2)
    E_GRPC_INVALID_ARGUMENT = make_grpc_error(3)
    E_GRPC_DEADLINE_EXCEEDED = make_grpc_error(4)
    E_GRPC_NOT_FOUND = make_grpc_error(5)
    E_GRPC_ALREADY_EXISTS = make_grpc_error(6)
    E_GRPC_PERMISSION_DENIED = make_grpc_error(7)
    E_GRPC_RESOURCE_EXHAUSTED = make_grpc_error(8)
    E_GRPC_FAILED_PRECONDITION = make_grpc_error(9)
    E_GRPC_ABORTED = make_grpc_error(10)
    E_GRPC_OUT_OF_RANGE = make_grpc_error(11)
    E_GRPC_UNIMPLEMENTED = make_grpc_error(12)
    E_GRPC_INTERNAL = make_grpc_error(13)
    E_GRPC_UNAVAILABLE = make_grpc_error(14)
    E_GRPC_DATA_LOSS = make_grpc_error(15)
    E_GRPC_UNAUTHENTICATED = make_grpc_error(16)

    # service errors
    E_INVALID_SESSION = make_service_error(1)
    E_OUT_OF_SPACE = make_service_error(2)
    E_THROTTLED = make_service_error(3)
    E_RESOURCE_EXHAUSTED = make_service_error(4)
    E_DISK_ALLOCATION_FAILED = make_service_error(5)
    E_MOUNT_CONFLICT = make_service_error(6)


def facility_string(code: int) -> str:
    try:
        return EFacility(facility_from_code(code)).name
    except Exception:
        return "FACILITY_UNKNOWN"


def severity_string(code: int) -> str:
    if succeeded(code):
        return "SEVERITY_SUCCESS"
    else:
        return "SEVERITY_ERROR"


def format_error_code(code: int) -> str:
    return "{} {} status:{}".format(
        severity_string(code),
        facility_string(code),
        status_from_code(code))
