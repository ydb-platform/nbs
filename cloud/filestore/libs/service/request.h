#pragma once

#include "public.h"

#include <google/protobuf/text_format.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace google::protobuf {
    class Message;
}

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVICE_METHODS(xxx, ...)                                    \
    xxx(CreateFileStore,                    __VA_ARGS__)                       \
    xxx(DestroyFileStore,                   __VA_ARGS__)                       \
    xxx(AlterFileStore,                     __VA_ARGS__)                       \
    xxx(ResizeFileStore,                    __VA_ARGS__)                       \
    xxx(DescribeFileStoreModel,             __VA_ARGS__)                       \
    xxx(GetFileStoreInfo,                   __VA_ARGS__)                       \
    xxx(ListFileStores,                     __VA_ARGS__)                       \
                                                                               \
    xxx(CreateSession,                      __VA_ARGS__)                       \
    xxx(DestroySession,                     __VA_ARGS__)                       \
                                                                               \
    xxx(AddClusterNode,                     __VA_ARGS__)                       \
    xxx(RemoveClusterNode,                  __VA_ARGS__)                       \
    xxx(ListClusterNodes,                   __VA_ARGS__)                       \
    xxx(AddClusterClients,                  __VA_ARGS__)                       \
    xxx(RemoveClusterClients,               __VA_ARGS__)                       \
    xxx(ListClusterClients,                 __VA_ARGS__)                       \
    xxx(UpdateCluster,                      __VA_ARGS__)                       \
    xxx(CreateCheckpoint,                   __VA_ARGS__)                       \
    xxx(DestroyCheckpoint,                  __VA_ARGS__)                       \
                                                                               \
    xxx(ExecuteAction,                      __VA_ARGS__)                       \
// FILESTORE_SERVICE_METHODS

#define FILESTORE_DATA_METHODS(xxx, ...)                                       \
    xxx(StatFileStore,                      __VA_ARGS__)                       \
                                                                               \
    xxx(SubscribeSession,                   __VA_ARGS__)                       \
    xxx(GetSessionEvents,                   __VA_ARGS__)                       \
    xxx(ResetSession,                       __VA_ARGS__)                       \
                                                                               \
    xxx(ResolvePath,                        __VA_ARGS__)                       \
    xxx(CreateNode,                         __VA_ARGS__)                       \
    xxx(UnlinkNode,                         __VA_ARGS__)                       \
    xxx(RenameNode,                         __VA_ARGS__)                       \
    xxx(AccessNode,                         __VA_ARGS__)                       \
    xxx(ListNodes,                          __VA_ARGS__)                       \
    xxx(ReadLink,                           __VA_ARGS__)                       \
                                                                               \
    xxx(SetNodeAttr,                        __VA_ARGS__)                       \
    xxx(GetNodeAttr,                        __VA_ARGS__)                       \
    xxx(SetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(GetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(ListNodeXAttr,                      __VA_ARGS__)                       \
    xxx(RemoveNodeXAttr,                    __VA_ARGS__)                       \
                                                                               \
    xxx(CreateHandle,                       __VA_ARGS__)                       \
    xxx(ConfirmCreateHandle,                __VA_ARGS__)                       \
    xxx(DestroyHandle,                      __VA_ARGS__)                       \
                                                                               \
    xxx(AcquireLock,                        __VA_ARGS__)                       \
    xxx(ReleaseLock,                        __VA_ARGS__)                       \
    xxx(TestLock,                           __VA_ARGS__)                       \
                                                                               \
    xxx(ReadData,                           __VA_ARGS__)                       \
    xxx(WriteData,                          __VA_ARGS__)                       \
    xxx(AllocateData,                       __VA_ARGS__)                       \
// FILESTORE_DATA_METHODS

#define FILESTORE_LOCAL_DATA_METHODS(xxx, ...)                                 \
    xxx(Fsync,                              __VA_ARGS__)                       \
    xxx(FsyncDir,                           __VA_ARGS__)                       \
// FILESTORE_LOCAL_DATA_METHODS

#define FILESTORE_SHARED_MEMORY_METHODS(xxx, ...)                              \
    xxx(Mmap,                               __VA_ARGS__)                       \
    xxx(Munmap,                             __VA_ARGS__)                       \
    xxx(PingMmapRegion,                     __VA_ARGS__)                       \
// FILESTORE_SHARED_MEMORY_METHODS

#define FILESTORE_DATA_SERVICE(xxx, ...)                                       \
    FILESTORE_DATA_METHODS(xxx,            __VA_ARGS__)                        \
    FILESTORE_LOCAL_DATA_METHODS(xxx,      __VA_ARGS__)                        \
// FILESTORE_DATA_SERVICE

#define FILESTORE_CONTROL_SERVICE(xxx, ...)                                    \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(PingSession,                        __VA_ARGS__)                       \
    FILESTORE_SERVICE_METHODS(xxx,          __VA_ARGS__)                       \
// FILESTORE_CONTROL_SERVICE

#define FILESTORE_SERVICE(xxx, ...)                                            \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(PingSession,                        __VA_ARGS__)                       \
    FILESTORE_SERVICE_METHODS(xxx,          __VA_ARGS__)                       \
    FILESTORE_DATA_METHODS(xxx,             __VA_ARGS__)                       \
    FILESTORE_LOCAL_DATA_METHODS(xxx,       __VA_ARGS__)                       \
// FILESTORE_SERVICE

#define FILESTORE_REMOTE_SERVICE(xxx, ...)                                     \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(PingSession,                        __VA_ARGS__)                       \
    FILESTORE_SERVICE_METHODS(xxx,          __VA_ARGS__)                       \
    FILESTORE_DATA_METHODS(xxx,             __VA_ARGS__)                       \
// FILESTORE_SERVICE

#define FILESTORE_ENDPOINT_METHODS(xxx, ...)                                   \
    xxx(StartEndpoint,                      __VA_ARGS__)                       \
    xxx(StopEndpoint,                       __VA_ARGS__)                       \
    xxx(ListEndpoints,                      __VA_ARGS__)                       \
    xxx(KickEndpoint,                       __VA_ARGS__)                       \
// FILESTORE_ENDPOINT_METHODS

#define FILESTORE_ENDPOINT_SERVICE(xxx, ...)                                   \
    xxx(Ping,                               __VA_ARGS__)                       \
    FILESTORE_ENDPOINT_METHODS(xxx,         __VA_ARGS__)                       \
// FILESTORE_ENDPOINT_SERVICE

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_REQUESTS(xxx, ...)                                           \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(PingSession,                        __VA_ARGS__)                       \
    FILESTORE_SERVICE_METHODS(xxx,          __VA_ARGS__)                       \
    FILESTORE_DATA_METHODS(xxx,             __VA_ARGS__)                       \
    FILESTORE_LOCAL_DATA_METHODS(xxx,       __VA_ARGS__)                       \
    xxx(GetSessionEventsStream,             __VA_ARGS__)                       \
    FILESTORE_ENDPOINT_METHODS(xxx,         __VA_ARGS__)                       \
// FILESTORE_REQUESTS

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_PROTO_REQUESTS(xxx, ...)                                     \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(PingSession,                        __VA_ARGS__)                       \
    FILESTORE_SERVICE_METHODS(xxx,          __VA_ARGS__)                       \
    FILESTORE_DATA_METHODS(xxx,             __VA_ARGS__)                       \
    FILESTORE_LOCAL_DATA_METHODS(xxx,       __VA_ARGS__)                       \
    FILESTORE_ENDPOINT_METHODS(xxx,         __VA_ARGS__)                       \
// FILESTORE_PROTO_REQUESTS

enum class EFileStoreRequest
{
    // These values are stored in profile logs and must not be changed.
    Ping = 0,
    PingSession = 1,
    CreateFileStore = 2,
    DestroyFileStore = 3,
    AlterFileStore = 4,
    ResizeFileStore = 5,
    DescribeFileStoreModel = 6,
    GetFileStoreInfo = 7,
    ListFileStores = 8,
    CreateSession = 9,
    DestroySession = 10,
    AddClusterNode = 11,
    RemoveClusterNode = 12,
    ListClusterNodes = 13,
    AddClusterClients = 14,
    RemoveClusterClients = 15,
    ListClusterClients = 16,
    UpdateCluster = 17,
    CreateCheckpoint = 18,
    DestroyCheckpoint = 19,
    ExecuteAction = 20,
    StatFileStore = 21,
    SubscribeSession = 22,
    GetSessionEvents = 23,
    ResetSession = 24,
    ResolvePath = 25,
    CreateNode = 26,
    UnlinkNode = 27,
    RenameNode = 28,
    AccessNode = 29,
    ListNodes = 30,
    ReadLink = 31,
    SetNodeAttr = 32,
    GetNodeAttr = 33,
    SetNodeXAttr = 34,
    GetNodeXAttr = 35,
    ListNodeXAttr = 36,
    RemoveNodeXAttr = 37,
    CreateHandle = 38,
    DestroyHandle = 39,
    AcquireLock = 40,
    ReleaseLock = 41,
    TestLock = 42,
    ReadData = 43,
    WriteData = 44,
    AllocateData = 45,
    Fsync = 46,
    FsyncDir = 47,
    GetSessionEventsStream = 48,
    StartEndpoint = 49,
    StopEndpoint = 50,
    ListEndpoints = 51,
    KickEndpoint = 52,
    DescribeData = 53,
    GenerateBlobIds = 54,
    AddData = 55,
    ReadBlob = 56,
    WriteBlob = 57,
    ConfirmAddData = 58,
    CancelAddData = 59,
    ConfirmCreateHandle = 60,
    Forget = 61,
    ForgetMulti = 62,
    OpenDir = 63,
    ReleaseDir = 64,
    FuseFlush = 65,
    FuseFsync = 66,
    FuseFsyncDir = 67,
    MAX = 68,
};

constexpr size_t FileStoreRequestCount = static_cast<size_t>(EFileStoreRequest::MAX);

const TString& GetFileStoreRequestName(EFileStoreRequest requestType);

////////////////////////////////////////////////////////////////////////////////

struct TRequestInfo
{
    ui64 RequestId;
    TString RequestName;
    TString FileSystemId;
    TString SessionId;
    TString ClientId;
};

IOutputStream& operator <<(IOutputStream& out, const TRequestInfo& info);

////////////////////////////////////////////////////////////////////////////////

ui64 CreateRequestId();

template <typename T>
constexpr EFileStoreRequest GetFileStoreServiceRequest();

template <typename T>
TString GetClientId(const T& request);

template <typename T>
TString GetSessionId(const T& request);

template <typename T>
ui64 GetRequestId(const T& request);

template <typename T>
TString GetFileSystemId(const T& request);

template <typename T>
void SetFileSystemId(TString fileSystemId, T& request);

template <typename T>
TString GetStorageMediaKind(const T& request);

template <typename T>
TString GetRequestName(const T& request);

template <typename T>
TRequestInfo GetRequestInfo(const T& request);

template <typename T>
consteval bool HasResponseHeaders();

////////////////////////////////////////////////////////////////////////////////

class TProtoMessagePrinter
{
public:
    TProtoMessagePrinter();

    virtual TString ToString(const google::protobuf::Message& message);

private:
    google::protobuf::TextFormat::Printer Printer;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRequest>
ui64 CalculateByteCount(const TProtoRequest&)
{
    return 0;
}

}   // namespace NCloud::NFileStore

#define FILESTORE_REQUEST_H
#include "request_i.h"
#undef FILESTORE_REQUEST_H
