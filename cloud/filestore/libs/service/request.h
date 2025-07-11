#pragma once

#include "public.h"

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
    xxx(ReadNodeRefs,                       __VA_ARGS__)                       \
                                                                               \
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

#define FILESTORE_DECLARE_REQUEST(name, ...) name,

enum class EFileStoreRequest
{
    FILESTORE_REQUESTS(FILESTORE_DECLARE_REQUEST)
    DescribeData,
    GenerateBlobIds,
    AddData,
    ReadBlob,
    WriteBlob,
    MAX
};

#undef FILESTORE_DECLARE_REQUEST

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

TString DumpMessage(const google::protobuf::Message& message);

}   // namespace NCloud::NFileStore

#define FILESTORE_REQUEST_H
#include "request_i.h"
#undef FILESTORE_REQUEST_H
