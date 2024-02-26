#pragma once

#include "public.h"

#include "context.h"
#include "fs.h"
#include "vfs.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/fsync_queue.h>
#include <cloud/filestore/libs/vfs/protos/session.pb.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/sglist_iter.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/libs/linux-headers/linux/fuse.h>

#include <util/generic/hash.h>
#include <util/string/printf.h>

#include <memory>

namespace NCloud::NFileStore::NVFSVhost {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle;

////////////////////////////////////////////////////////////////////////////////

class TFileSystem final
    : public std::enable_shared_from_this<TFileSystem>
    , public IFileSystem
{
private:
    static constexpr ui64 MissingNodeId = -1;

private:
    const NVFS::TVFSConfigPtr Config;
    const NClient::ISessionPtr Session;
    const ISchedulerPtr Scheduler;
    const IRequestStatsRegistryPtr StatsRegistry;
    const IProfileLogPtr ProfileLog;
    const TString LogTag;

    NVFS::TFileSystemConfigPtr FsConfig;
    NVFS::TFSyncQueue FSyncQueue;

    TMutex Lock;
    THashMap<ui64, std::shared_ptr<TDirectoryHandle>> DirectoryHandles;

    NProto::TVfsSessionState SessionState;
    NProto::EStorageMediaKind StorageMediaKind = NProto::STORAGE_MEDIA_DEFAULT;

    TLog Log;
    IRequestStatsPtr RequestStats;

public:
    TFileSystem(
            NVFS::TVFSConfigPtr config,
            NClient::ISessionPtr session,
            ILoggingServicePtr logging,
            ISchedulerPtr scheduler,
            IRequestStatsRegistryPtr statsRegistry,
            IProfileLogPtr profileLog)
        : Config(std::move(config))
        , Session(std::move(session))
        , Scheduler(std::move(scheduler))
        , StatsRegistry(std::move(statsRegistry))
        , ProfileLog(std::move(profileLog))
        , LogTag(Sprintf("[f:%s][c:%s]",
            Config->GetFileSystemId().c_str(),
            Config->GetClientId().c_str()))
        , FSyncQueue(Config->GetFileSystemId(), logging)
        , Log(logging->CreateLog("NFS_FUSE"))
    {}

    NThreading::TFuture<NProto::TError> Start() override;
    NThreading::TFuture<NProto::TError> Stop() override;
    NThreading::TFuture<NProto::TError> Alter(
        bool readOnly,
        ui64 mountSeqNumber) override;

    NThreading::TFuture<NProto::TError> Process(
        TVfsRequestPtr request) override;

public:
    NThreading::TFuture<NProto::TError> Init(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Destroy(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> StatFs(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);
    //
    // Nodes
    //

    NThreading::TFuture<NProto::TError> Lookup(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Access(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Forget(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> BatchForget(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> MkDir(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> RmDir(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> MkNode(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Unlink(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Rename(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Link(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> SymLink(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> ReadLink(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

/*
    //
    // Data
    //

    NThreading::TFuture<NProto::TError> Open(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Release(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Read(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> OpenDir(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> ReleaseDir(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> ReadDir(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> ReadDirPlus(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Write(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> Flush(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> FSync(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> FAllocate(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    //
    // Index
    //

    NThreading::TFuture<NProto::TError> Create(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> FSyncDir(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    //
    // Attr
    //

    NThreading::TFuture<NProto::TError> GetAttr(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> SetAttr(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> ListXattr(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> GetXattr(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> SetXattr(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> RemoveXattr(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    //
    // Locks
    //

    NThreading::TFuture<NProto::TError> GetLk(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> SetLk(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

    NThreading::TFuture<NProto::TError> SetLkw(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);
*/
private:
    NProto::TError HandleCreateSession(
        const TCallContextPtr& callContext,
        const NThreading::TFuture<NProto::TCreateSessionResponse>& future);

    NThreading::TFuture<NProto::TError> ResetSessionState(
        const TCallContextPtr& callContext);

    template <typename T>
    std::shared_ptr<T> CreateRequest()
    {
        return std::make_shared<T>();
    }

    template <typename T>
    std::shared_ptr<T> CreateRequest(ui64 node)
    {
        auto request = StartRequest<T>();
        request->SetNodeId(node);

        return request;
    }

    static bool CheckFailed(
        const std::shared_ptr<TFileSystem>& self,
        const TVfsRequestContextPtr& ctx,
        const NProto::TError& error)
    {
        return !self || self->CheckFailed(ctx, error);
    }

    bool CheckFailed(const TVfsRequestContextPtr& ctx, const NProto::TError& error);

    void ReplyOk(const TVfsRequestContextPtr& ctx);
    void ReplyError(const TVfsRequestContextPtr& ctx, int error);
    NProto::TError ReplyEntry(
        const TVfsRequestContextPtr& ctx,
        const NProto::TNodeAttr& attrs);

    template<typename... Args>
    NProto::TError Reply(const TVfsRequestContextPtr& ctx, const Args&... args);
};

}   // namespace NCloud::NFileStore::NVFSVhost
