#pragma once

#include "public.h"

#include <cloud/storage/core/protos/error.pb.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>

#include <tuple>
#include <type_traits>

namespace NCloud {

// TODO: decide how to fix this
namespace NFileStore::NProto {
    using namespace NCloud::NProto;
}
namespace NBlockStore::NProto {
    using namespace NCloud::NProto;
}

////////////////////////////////////////////////////////////////////////////////
// We should combine errors from different facilities:
// * Generic errors
// * System errors (file system, network etc)
// * gRPC errors
// * KiKiMR errors
// * Application errors (storage, service etc)

enum ESeverityCode
{
    SEVERITY_SUCCESS        = 0,
    SEVERITY_ERROR          = 1,
};

enum EFacilityCode
{
    FACILITY_NULL           = 0,
    FACILITY_SYSTEM         = 1,
    FACILITY_GRPC           = 2,
    FACILITY_KIKIMR         = 3,
    FACILITY_SCHEMESHARD    = 4,
    FACILITY_BLOCKSTORE     = 5,
    FACILITY_TXPROXY        = 6,
    FACILITY_FILESTORE      = 7,
    FACILITY_RDMA           = 8,
    FACILITY_MAX            // should be the last one
};

#define SUCCEEDED(code)              ((ui32(code) & 0x80000000u) == 0)
#define FAILED(code)                 ((ui32(code) & 0x80000000u) != 0)
#define FACILITY_FROM_CODE(code)     ((ui32(code) & 0x7FFFFFFFu) >> 16)
#define STATUS_FROM_CODE(code)       ((ui32(code) & 0x0000FFFFu))

#define MAKE_RESULT_CODE(severity, facility, status)                           \
    ( ((ui32(severity) & 0x00000001u) << 31)                                   \
    | ((ui32(facility) & 0x00007FFFu) << 16)                                   \
    | ((ui32(status)   & 0x0000FFFFu)      )                                   \
    )                                                                          \
// MAKE_RESULT_CODE

#define MAKE_SUCCESS(status) \
    MAKE_RESULT_CODE(SEVERITY_SUCCESS, FACILITY_NULL, status)

#define MAKE_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_NULL, status)

#define MAKE_SYSTEM_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_SYSTEM, status)

#define MAKE_GRPC_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_GRPC, status)

#define MAKE_KIKIMR_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_KIKIMR, status)

#define MAKE_SCHEMESHARD_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_SCHEMESHARD, status)

#define MAKE_TXPROXY_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_TXPROXY, status)

#define MAKE_BLOCKSTORE_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_BLOCKSTORE, status)

#define MAKE_FILESTORE_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_FILESTORE, status)

#define MAKE_RDMA_ERROR(status) \
    MAKE_RESULT_CODE(SEVERITY_ERROR, FACILITY_RDMA, status)

////////////////////////////////////////////////////////////////////////////////

enum EWellKnownResultCodes: ui32
{
    S_OK                         = MAKE_SUCCESS(0),
    S_FALSE                      = MAKE_SUCCESS(1),
    S_ALREADY                    = MAKE_SUCCESS(2),

    E_FAIL                       = MAKE_ERROR(0),
    E_ARGUMENT                   = MAKE_ERROR(1),
    E_REJECTED                   = MAKE_ERROR(2),
    // see MDB-11177#5fe20bc27e06002b58fe3eec
    // E_IO                      = MAKE_ERROR(3),
    E_INVALID_STATE              = MAKE_ERROR(4),
    E_TIMEOUT                    = MAKE_ERROR(5),
    E_NOT_FOUND                  = MAKE_ERROR(6),
    E_UNAUTHORIZED               = MAKE_ERROR(7),
    E_NOT_IMPLEMENTED            = MAKE_ERROR(8),
    E_ABORTED                    = MAKE_ERROR(9),
    E_TRY_AGAIN                  = MAKE_ERROR(10),
    E_IO                         = MAKE_ERROR(11),
    E_CANCELLED                  = MAKE_ERROR(12),
    E_IO_SILENT                  = MAKE_ERROR(13),  // XXX: need to keep this for legacy clients
    E_RETRY_TIMEOUT              = MAKE_ERROR(14),
    E_PRECONDITION_FAILED        = MAKE_ERROR(15),

    E_GRPC_CANCELLED             = MAKE_GRPC_ERROR(1),
    E_GRPC_UNKNOWN               = MAKE_GRPC_ERROR(2),
    E_GRPC_INVALID_ARGUMENT      = MAKE_GRPC_ERROR(3),
    E_GRPC_DEADLINE_EXCEEDED     = MAKE_GRPC_ERROR(4),
    E_GRPC_NOT_FOUND             = MAKE_GRPC_ERROR(5),
    E_GRPC_ALREADY_EXISTS        = MAKE_GRPC_ERROR(6),
    E_GRPC_PERMISSION_DENIED     = MAKE_GRPC_ERROR(7),
    E_GRPC_RESOURCE_EXHAUSTED    = MAKE_GRPC_ERROR(8),
    E_GRPC_FAILED_PRECONDITION   = MAKE_GRPC_ERROR(9),
    E_GRPC_ABORTED               = MAKE_GRPC_ERROR(10),
    E_GRPC_OUT_OF_RANGE          = MAKE_GRPC_ERROR(11),
    E_GRPC_UNIMPLEMENTED         = MAKE_GRPC_ERROR(12),
    E_GRPC_INTERNAL              = MAKE_GRPC_ERROR(13),
    E_GRPC_UNAVAILABLE           = MAKE_GRPC_ERROR(14),
    E_GRPC_DATA_LOSS             = MAKE_GRPC_ERROR(15),
    E_GRPC_UNAUTHENTICATED       = MAKE_GRPC_ERROR(16),

    E_BS_INVALID_SESSION         = MAKE_BLOCKSTORE_ERROR(1),
    E_BS_OUT_OF_SPACE            = MAKE_BLOCKSTORE_ERROR(2),
    E_BS_THROTTLED               = MAKE_BLOCKSTORE_ERROR(3),
    E_BS_RESOURCE_EXHAUSTED      = MAKE_BLOCKSTORE_ERROR(4),
    E_BS_DISK_ALLOCATION_FAILED  = MAKE_BLOCKSTORE_ERROR(5),
    E_BS_MOUNT_CONFLICT          = MAKE_BLOCKSTORE_ERROR(6),

    E_FS_IO                      = MAKE_FILESTORE_ERROR(0),
    E_FS_PERM                    = MAKE_FILESTORE_ERROR(1),
    E_FS_NOENT                   = MAKE_FILESTORE_ERROR(2),
    E_FS_NXIO                    = MAKE_FILESTORE_ERROR(3),
    E_FS_ACCESS                  = MAKE_FILESTORE_ERROR(4),
    E_FS_EXIST                   = MAKE_FILESTORE_ERROR(5),
    E_FS_XDEV                    = MAKE_FILESTORE_ERROR(6),
    E_FS_NODEV                   = MAKE_FILESTORE_ERROR(7),
    E_FS_NOTDIR                  = MAKE_FILESTORE_ERROR(8),
    E_FS_ISDIR                   = MAKE_FILESTORE_ERROR(9),
    E_FS_INVAL                   = MAKE_FILESTORE_ERROR(10),
    E_FS_FBIG                    = MAKE_FILESTORE_ERROR(11),
    E_FS_NOSPC                   = MAKE_FILESTORE_ERROR(12),
    E_FS_ROFS                    = MAKE_FILESTORE_ERROR(13),
    E_FS_MLINK                   = MAKE_FILESTORE_ERROR(14),
    E_FS_NAMETOOLONG             = MAKE_FILESTORE_ERROR(15),
    E_FS_NOTEMPTY                = MAKE_FILESTORE_ERROR(16),
    E_FS_DQUOT                   = MAKE_FILESTORE_ERROR(17),
    E_FS_STALE                   = MAKE_FILESTORE_ERROR(18),
    E_FS_REMOTE                  = MAKE_FILESTORE_ERROR(19),
    E_FS_BADHANDLE               = MAKE_FILESTORE_ERROR(20),
    E_FS_NOTSUPP                 = MAKE_FILESTORE_ERROR(21),

    E_FS_WOULDBLOCK              = MAKE_FILESTORE_ERROR(30),
    E_FS_NOLCK                   = MAKE_FILESTORE_ERROR(31),

    E_FS_INVALID_SESSION         = MAKE_FILESTORE_ERROR(100),
    E_FS_OUT_OF_SPACE            = MAKE_FILESTORE_ERROR(101),
    E_FS_THROTTLED               = MAKE_FILESTORE_ERROR(102),

    E_RDMA_UNAVAILABLE           = MAKE_RDMA_ERROR(1),
};

////////////////////////////////////////////////////////////////////////////////

// error classification used in retry policies
enum class EErrorKind
{
    Success,
    ErrorAborted,
    ErrorFatal,
    ErrorRetriable,
    ErrorSession,
};

bool IsConnectionError(const NProto::TError& e);
EErrorKind GetErrorKind(const NProto::TError& e);

// error classification used for logging and stats
enum class EDiagnosticsErrorKind
{
    Success,
    ErrorAborted,
    ErrorFatal,
    ErrorRetriable,
    ErrorThrottling,
    ErrorWriteRejectedByCheckpoint,
    ErrorSession,
    ErrorSilent,
    Max,
};

EDiagnosticsErrorKind GetDiagnosticsErrorKind(const NProto::TError& e);

////////////////////////////////////////////////////////////////////////////////

class TServiceError
    : public yexception
{
private:
    const ui32 Code;

public:
    explicit TServiceError(ui32 code)
        : Code(code)
    {}

    TServiceError(const NProto::TError& error)
        : Code(error.GetCode())
    {
        Append(error.GetMessage());
    }

    ui32 GetCode() const
    {
        return Code;
    }

    TStringBuf GetMessage() const
    {
        return AsStrBuf();
    }
};

////////////////////////////////////////////////////////////////////////////////

TString FormatError(const NProto::TError& e);
TString FormatResultCode(ui32 code);
NJson::TJsonValue FormatErrorJson(const NProto::TError& e);

NProto::TError MakeError(ui32 code, TString message = {}, ui32 flags = 0);

template <typename T>
concept TAcceptsError = requires(T a)
{
    { *a.MutableError() = MakeError(S_OK) };
};

template <typename T>
T ErrorResponse(ui32 code, TString message)
{
    T response;
    *response.MutableError() = MakeError(code, std::move(message));
    return response;
}

inline bool HasError(const NProto::TError& e)
{
    return FAILED(e.GetCode());
}

template <typename T>
bool HasError(const T& response)
{
    return response.HasError()
        && HasError(response.GetError());
}

inline void CheckError(const NProto::TError& error)
{
    if (HasError(error)) {
        ythrow TServiceError(error.GetCode()) << error.GetMessage();
    }
}

template <typename T>
void CheckError(const T& response)
{
    if (HasError(response)) {
        ythrow TServiceError(response.GetError().GetCode())
            << response.GetError().GetMessage();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TResultOrError
{
private:
    T Result;
    NProto::TError Error;

public:
    TResultOrError(T result)
        : Result(std::move(result))
    {}

    TResultOrError(NProto::TError error)
        : Result{}
        , Error(std::move(error))
    {}

    const T& GetResult() const
    {
        return Result;
    }

    T ExtractResult()
    {
        return std::move(Result);
    }

    const NProto::TError& GetError() const
    {
        return Error;
    }

    bool HasError() const
    {
        // emulate protobuf message ('true' means Error-field exists)
        return true;
    }

    // Structured bindings support

    template<int I>
    const auto& get() const
    {
        if constexpr (I == 0) {
            return Result;
        }

        if constexpr (I == 1) {
            return Error;
        }
    }

    template<int I>
    auto&& get() &&
    {
        if constexpr (I == 0) {
            return std::move(Result);
        }

        if constexpr (I == 1) {
            return std::move(Error);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TResultOrError<void>
{
private:
    const NProto::TError Error;

public:
    TResultOrError()
    {}

    TResultOrError(NProto::TError error)
        : Error(std::move(error))
    {}

    const NProto::TError& GetError() const
    {
        return Error;
    }

    bool HasError() const
    {
        // emulate protobuf message ('true' means Error-field exists)
        return true;
    }

    // Explicitly disallow structured bindings
    template<int I> const auto& get() const = delete;
};

////////////////////////////////////////////////////////////////////////////////

class TErrorResponse
{
private:
    const ui32 Code;
    const TString Message;

public:
    TErrorResponse(ui32 code, TString message = {})
        : Code(code)
        , Message(std::move(message))
    {}

    TErrorResponse(const NProto::TError& e)
        : Code(e.GetCode())
        , Message(e.GetMessage())
    {}

    TErrorResponse(const TServiceError& e)
        : Code(e.GetCode())
        , Message(e.GetMessage())
    {}

    template <TAcceptsError T>
    operator T() const
    {
        return ErrorResponse<T>(Code, Message);
    }

    template <typename T>
    operator TResultOrError<T>() const
    {
        return MakeError(Code, Message);
    }

    operator NProto::TError() const
    {
        return MakeError(Code, Message);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse, typename T>
TResponse SafeExecute(T&& block)
{
    try {
        return block();
    } catch (const TServiceError& e) {
        return TErrorResponse(e);
    } catch (const TIoSystemError& e) {
        return TErrorResponse(MAKE_SYSTEM_ERROR(e.Status()), e.what());
    } catch (...) {
        return TErrorResponse(E_FAIL, CurrentExceptionMessage());
    }
}

template <typename T>
T ExtractResponse(NThreading::TFuture<T>& future)
{
    return SafeExecute<T>([&] {
        return future.ExtractValue();
    });
}

template <typename T>
TResultOrError<T> ResultOrError(NThreading::TFuture<T>& future)
{
    return SafeExecute<TResultOrError<T>>([&] {
        return future.ExtractValue();
    });
}

inline TResultOrError<void> ResultOrError(NThreading::TFuture<void>& future)
{
    return SafeExecute<TResultOrError<void>>([&] {
        future.TryRethrow();
        return NProto::TError();
    });
}

}   // namespace NCloud

////////////////////////////////////////////////////////////////////////////////

namespace std {

    // Structured bindings support for TResultOrError<T>

    template <typename T>
    struct tuple_size<NCloud::TResultOrError<T>>
        : integral_constant<size_t, 2>
    {};

    template <typename T>
    struct tuple_element<0, NCloud::TResultOrError<T>> {
        using type = T;
    };

    template <typename T>
    struct tuple_element<1, NCloud::TResultOrError<T>> {
        using type = NCloud::NProto::TError;
    };

}   // namespace std
