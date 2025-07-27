#pragma once

#include "public.h"

#include "cluster.h"
#include "config.h"
#include "index.h"
#include "session.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/string.h>
#include <util/system/mutex.h>
#include <util/system/rwlock.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DATA_METHODS_LOCAL_SYNC(xxx, ...)                            \
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
    xxx(AllocateData,                       __VA_ARGS__)                       \
                                                                               \
    xxx(Fsync,                              __VA_ARGS__)                       \
    xxx(FsyncDir,                           __VA_ARGS__)                       \
// FILESTORE_DATA_METHODS_LOCAL_SYNC

#define FILESTORE_DATA_METHODS_LOCAL_ASYNC(xxx, ...)                           \
    xxx(ReadData,                           __VA_ARGS__)                       \
    xxx(WriteData,                          __VA_ARGS__)                       \
// FILESTORE_DATA_METHODS_LOCAL_ASYNC

#define FILESTORE_SERVICE_LOCAL_SYNC(xxx, ...)                                 \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(PingSession,                        __VA_ARGS__)                       \
    FILESTORE_SERVICE_METHODS(xxx,          __VA_ARGS__)                       \
    FILESTORE_DATA_METHODS_LOCAL_SYNC(xxx,  __VA_ARGS__)                       \
    xxx(ReadNodeRefs,                       __VA_ARGS__)                       \
// FILESTORE_SERVICE_LOCAL_SYNC

#define FILESTORE_SERVICE_LOCAL_ASYNC(xxx, ...)                                \
    FILESTORE_DATA_METHODS_LOCAL_ASYNC(xxx,  __VA_ARGS__)                      \
// FILESTORE_SERVICE_LOCAL_SYNC

////////////////////////////////////////////////////////////////////////////////

class TLocalFileSystem final
    : public std::enable_shared_from_this<TLocalFileSystem>
{
    using TSessionList = TList<TSessionPtr>;

private:
    const TLocalFileStoreConfigPtr Config;
    const TFsPath RootPath;
    const TFsPath StatePath;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IFileIOServicePtr FileIOService;

    NProto::TFileStore Store;
    TLog Log;

    TSessionList SessionsList;
    THashMap<TString, TSessionList::iterator> SessionsById;
    THashMap<TString, TSessionList::iterator> SessionsByClient;
    TRWMutex SessionsLock;

    TCluster Cluster;
    TMutex ClusterLock;

public:
    TLocalFileSystem(
        TLocalFileStoreConfigPtr config,
        NProto::TFileStore store,
        TFsPath root,
        TFsPath statePath,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IFileIOServicePtr fileIOService);

#define FILESTORE_DECLARE_METHOD_SYNC(name, ...)                               \
    NProto::T##name##Response name(                                            \
        const NProto::T##name##Request& request);                              \
// FILESTORE_DECLARE_METHOD_SYNC

#define FILESTORE_DECLARE_METHOD_ASYNC(name, ...)               \
    NThreading::TFuture<NProto::T##name##Response> name##Async( \
        NProto::T##name##Request& request,                      \
        NProto::TProfileLogRequestInfo& logRequest);            \
    // FILESTORE_DECLARE_METHOD_SYNC

    FILESTORE_SERVICE_LOCAL_SYNC(FILESTORE_DECLARE_METHOD_SYNC)
    FILESTORE_SERVICE_LOCAL_ASYNC(FILESTORE_DECLARE_METHOD_ASYNC)
    FILESTORE_DECLARE_METHOD_ASYNC(ReadDataLocal)
    FILESTORE_DECLARE_METHOD_ASYNC(WriteDataLocal)

#undef FILESTORE_DECLARE_METHOD_SYNC
#undef FILESTORE_DECLARE_METHOD_ASYNC

    NProto::TFileStore GetConfig() const
    {
        return Store;
    }

    void SetConfig(NProto::TFileStore store)
    {
        Y_ABORT_UNLESS(store.GetFileSystemId() == Store.GetFileSystemId());
        Store = std::move(store);
    }

private:

    void ScheduleCleanupSessions();
    void CleanupSessions();

    template <typename T>
    TSessionPtr GetSession(const T& request)
    {
        const auto& clientId = GetClientId(request);
        const auto& sessionId = GetSessionId(request);
        const auto sessionSeqNo = GetSessionSeqNo(request);

        return GetSession(clientId, sessionId, sessionSeqNo);
    }

    TSessionPtr GetSession(
        const TString& clientId,
        const TString& sessionId,
        ui64 seqNo);

    TSessionPtr FindSession(
        const TString& clientId,
        const TString& sessionId,
        ui64 seqNo);

    void RemoveSession(const TString& sessionId, ui64 seqNo);
};

////////////////////////////////////////////////////////////////////////////////

void ConvertStats(const TFileStat& stat, NProto::TNodeAttr& node);

}   // namespace NCloud::NFileStore
