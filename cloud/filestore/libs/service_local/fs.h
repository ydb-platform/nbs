#pragma once

#include "public.h"

#include "cluster.h"
#include "config.h"
#include "index.h"
#include "session.h"

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

class TLocalFileSystem final
    : public std::enable_shared_from_this<TLocalFileSystem>
{
    using TSessionList = TList<TSessionPtr>;

private:
    const TLocalFileStoreConfigPtr Config;
    const TFsPath Root;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;

    NProto::TFileStore Store;
    TLog Log;

    TLocalIndexPtr Index;

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
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging);

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    NProto::T##name##Response name(                                            \
        const NProto::T##name##Request& request);                              \
// FILESTORE_DECLARE_METHOD

    FILESTORE_SERVICE(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

    NProto::TFileStore GetConfig() const
    {
        return Store;
    }

    void SetConfig(NProto::TFileStore store)
    {
        Y_ABORT_UNLESS(store.GetFileSystemId() == Store.GetFileSystemId());
        Store = std::move(store);
    }

    bool HasActiveSessions() const
    {
        TReadGuard guard(SessionsLock);
        return !SessionsList.empty();
    }

private:
    void InitIndex();

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
