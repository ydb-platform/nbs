#include "fs_impl.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NFileStore::NVFSVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

using THandler = TFuture<NProto::TError> (TFileSystem::*)(
        TVfsRequestContextPtr ctx,
        const struct fuse_in_header& header,
        TSgListInputIterator& in);

struct THandlerEntry
{
    TStringBuf Name;
    THandler Handler = nullptr;
    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    bool Oneway = false;
};

constexpr auto InitHandlers() {
    constexpr size_t MAX_HANDLERS = 64;

    std::array<THandlerEntry, MAX_HANDLERS> handlers;
    handlers[FUSE_INIT]     = {"INIT",    &TFileSystem::Init,    EFileStoreRequest::ResetSession};
    handlers[FUSE_DESTROY]  = {"DESTROY", &TFileSystem::Destroy, EFileStoreRequest::ResetSession};
    handlers[FUSE_STATFS]   = {"STATFS",  &TFileSystem::StatFs,  EFileStoreRequest::StatFileStore};

    handlers[FUSE_ACCESS]   = {"ACCESS",   &TFileSystem::Access,  EFileStoreRequest::AccessNode};
    handlers[FUSE_FORGET]   = {"FORGET",   &TFileSystem::Forget,  EFileStoreRequest::MAX, true};
    handlers[FUSE_BATCH_FORGET] = {"BFORGET", &TFileSystem::BatchForget, EFileStoreRequest::MAX, true};

    handlers[FUSE_MKDIR]    = {"MKDIR",    &TFileSystem::MkDir,    EFileStoreRequest::CreateNode};
    handlers[FUSE_RMDIR]    = {"RMDIR",    &TFileSystem::RmDir,    EFileStoreRequest::UnlinkNode};
    handlers[FUSE_MKNOD]    = {"MKNOD",    &TFileSystem::MkNode,   EFileStoreRequest::CreateNode};
    handlers[FUSE_UNLINK]   = {"UNLINK",   &TFileSystem::Unlink,   EFileStoreRequest::UnlinkNode};
    handlers[FUSE_RENAME]   = {"RENAME",   &TFileSystem::Rename,   EFileStoreRequest::RenameNode};
    handlers[FUSE_RENAME2]  = {"RENAME2",  &TFileSystem::Rename,   EFileStoreRequest::RenameNode};
    handlers[FUSE_LINK]     = {"LINK",     &TFileSystem::Link,     EFileStoreRequest::CreateNode};
    handlers[FUSE_SYMLINK]  = {"SYMLINK",  &TFileSystem::SymLink,  EFileStoreRequest::CreateNode};
    handlers[FUSE_READLINK] = {"READLINK", &TFileSystem::ReadLink, EFileStoreRequest::ReadLink};

/*
    handlers[FUSE_LOOKUP]   = {"LOOKUP",   &TFileSystem::Lookup,  EFileStoreRequest::AccessNode};
    handlers[FUSE_OPEN]      = {"OPEN",     &TFileSystem::Open,       EFileStoreRequest::CreateHandle};
    handlers[FUSE_RELEASE]   = {"RELEASE",  &TFileSystem::Release,    EFileStoreRequest::DestroyHandle};
    handlers[FUSE_READ]      = {"READ",     &TFileSystem::Read,       EFileStoreRequest::ReadData};
    handlers[FUSE_WRITE]     = {"WRITE",    &TFileSystem::Write,      EFileStoreRequest::WriteData};
    handlers[FUSE_FALLOCATE] = {"FALLOCATE", &TFileSystem::FAllocate, EFileStoreRequest::AllocateData};
    handlers[FUSE_FLUSH]     = {"FLUSH",    &TFileSystem::Flush,      EFileStoreRequest::MAX};
    handlers[FUSE_FSYNC]     = {"FSYNC",    &TFileSystem::FSync,      EFileStoreRequest::MAX};

    handlers[FUSE_GETATTR]  = {"GETATTR",  &TFileSystem::GetAttr, EFileStoreRequest::GetNodeAttr};
    handlers[FUSE_SETATTR]  = {"SETATTR",  &TFileSystem::SetAttr, EFileStoreRequest::SetNodeAttr};

    handlers[FUSE_OPENDIR]      = {"OPENDIR",     &TFileSystem::OpenDir,     EFileStoreRequest::MAX};
    handlers[FUSE_READDIR]      = {"READDIR",     &TFileSystem::ReadDir,     EFileStoreRequest::MAX};
    handlers[FUSE_RELEASEDIR]   = {"RELEASEDIR",  &TFileSystem::ReleaseDir,  EFileStoreRequest::MAX};
    handlers[FUSE_READDIRPLUS]  = {"READDIRPLUS", &TFileSystem::ReadDirPlus, EFileStoreRequest::ListNodes};

    handlers[FUSE_CREATE]   = {"CREATE",   &TFileSystem::Create,   EFileStoreRequest::CreateNode};
    handlers[FUSE_FSYNCDIR] = {"FSYNCDIR", &TFileSystem::FSyncDir, EFileStoreRequest::MAX};

    handlers[FUSE_LISTXATTR]   = {"LISTXATTR", &TFileSystem::ListXattr, EFileStoreRequest::ListNodeXAttr};
    handlers[FUSE_GETXATTR]    = {"GETXATTR",  &TFileSystem::GetXattr, EFileStoreRequest::GetNodeXAttr};
    handlers[FUSE_SETXATTR]    = {"SETXATTR",  &TFileSystem::SetXattr, EFileStoreRequest::SetNodeXAttr};
    handlers[FUSE_REMOVEXATTR] = {"RMXATTR",   &TFileSystem::RemoveXattr, EFileStoreRequest::RemoveNodeXAttr};

    handlers[FUSE_GETLK]  = {"GETLK",  &TFileSystem::GetLk, EFileStoreRequest::TestLock};
    // appropriate acquire/release type will be set in the handler
    handlers[FUSE_SETLK]  = {"SETLK",  &TFileSystem::SetLk, EFileStoreRequest::MAX};
    handlers[FUSE_SETLKW] = {"SETLKW", &TFileSystem::SetLkw, EFileStoreRequest::MAX};
*/
    return handlers;
}

const THandlerEntry* LookupHandler(ui32 opcode) {
    static constexpr auto handlers = InitHandlers();
    if (opcode >= handlers.size() || !handlers[opcode].Name) {
        return nullptr;
    }

    return &handlers[opcode];
}

////////////////////////////////////////////////////////////////////////////////

TString Dump(const fuse_in_header& header)
{
    return Sprintf("[len: %d op: %d unique: %lu node: %lu uid: %d gid: %d extlen: %hu}",
        header.len,
        header.opcode,
        header.unique,
        header.nodeid,
        header.uid,
        header.gid,
        header.total_extlen);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TFileSystem::Start()
{
    RequestStats = StatsRegistry->GetFileSystemStats(
        Config->GetFileSystemId(),
        Config->GetClientId());

    auto callContext = MakeIntrusive<TCallContext>(
        Config->GetFileSystemId(),
        CreateRequestId());
    callContext->RequestType = EFileStoreRequest::CreateSession;
    RequestStats->RequestStarted(Log, *callContext);

    auto weak = weak_from_this();
    return Session->CreateSession(
        Config->GetReadOnly(),
        Config->GetMountSeqNumber()).Apply(
            [=, weak = std::move(weak)] (const auto& future) -> NProto::TError {
                auto self = weak.lock();
                if (!self) {
                    return MakeError(E_INVALID_STATE, "Driver is destroyed");
                }

                return self->HandleCreateSession(callContext, future);
        });
}

NProto::TError TFileSystem::HandleCreateSession(
    const TCallContextPtr& callContext,
    const TFuture<NProto::TCreateSessionResponse>& future)
{
    NProto::TError error;

    try {
        const auto& response = future.GetValue();
        callContext->Error = response.GetError();

        RequestStats->RequestCompleted(Log, *callContext);
        if (HasError(response)) {
            STORAGE_ERROR("%s failed to create session: %s",
                LogTag.c_str(),
                FormatError(response.GetError()).c_str());

            return response.GetError();
        }

        const auto& filestore = response.GetFileStore();
        ui64 rawMedia = filestore.GetStorageMediaKind();
        if (!NProto::EStorageMediaKind_IsValid(rawMedia)) {
            STORAGE_WARN("%s got unsupported media kind %lu",
                LogTag.c_str(),
                rawMedia);
        }

        switch (rawMedia) {
            case NProto::STORAGE_MEDIA_SSD:
                StorageMediaKind = NProto::STORAGE_MEDIA_SSD;
                break;
            default:
                StorageMediaKind = NProto::STORAGE_MEDIA_HDD;
                break;
        }

        StatsRegistry->SetFileSystemMediaKind(
            Config->GetFileSystemId(),
            Config->GetClientId(),
            StorageMediaKind);
        StatsRegistry->RegisterUserStats(
            filestore.GetCloudId(),
            filestore.GetFolderId(),
            Config->GetFileSystemId(),
            Config->GetClientId());

        const TString& state = response.GetSession().GetSessionState();
        if (!SessionState.ParseFromString(state)) {
            // TODO: Crit event
            error = MakeError(E_INVALID_STATE, "failed to parse session state");
            STORAGE_ERROR("%s failed to start: %s",
                LogTag.c_str(),
                FormatError(error).c_str());

            return error;
        }

        STORAGE_INFO("%s starting %s session",
            LogTag.c_str(),
            state.empty() ? "new" : "existing");

        NProto::TFileSystemConfig fsConfig;
        fsConfig.SetFileSystemId(filestore.GetFileSystemId());
        fsConfig.SetBlockSize(filestore.GetBlockSize());

        FsConfig = std::make_shared<NVFS::TFileSystemConfig>(
            std::move(fsConfig));

    } catch (const TServiceError& e) {
        error = MakeError(e.GetCode(), TString(e.GetMessage()));
        STORAGE_ERROR("%s failed to start: %s",
            LogTag.c_str(),
            FormatError(error).c_str());
    } catch (...) {
        error = MakeError(E_FAIL, CurrentExceptionMessage());
        STORAGE_ERROR("%s failed to start: %s",
            LogTag.c_str(),
            FormatError(error).c_str());
    }

    return error;
}

TFuture<NProto::TError> TFileSystem::Stop()
{
    auto callContext = MakeIntrusive<TCallContext>(
        Config->GetFileSystemId(),
        CreateRequestId());
    callContext->RequestType = EFileStoreRequest::DestroySession;
    RequestStats->RequestStarted(Log, *callContext);

    auto weak = weak_from_this();
    return Session->DestroySession()
        .Apply([=, weak = std::move(weak)] (const auto& future) {
            auto self = weak.lock();
            if (!self) {
                return NProto::TError{};
            }

            const auto& response = future.GetValue();
            if (HasError(response)) {
                callContext->Error = response.GetError();
            }
            RequestStats->RequestCompleted(Log, *callContext);

            StatsRegistry->Unregister(
                Config->GetFileSystemId(),
                Config->GetClientId());

            return NProto::TError{};
        });
}

TFuture<NProto::TError> TFileSystem::Alter(bool readOnly, ui64 mountSeqNumber)
{
    auto callContext = MakeIntrusive<TCallContext>(
        Config->GetFileSystemId(),
        CreateRequestId());
    callContext->RequestType = EFileStoreRequest::CreateSession;
    RequestStats->RequestStarted(Log, *callContext);

    auto weak = weak_from_this();
    return Session->AlterSession(readOnly, mountSeqNumber)
        .Apply([=, weak = std::move(weak)] (const auto& future) {
            auto self = weak.lock();
            if (!self) {
                return MakeError(E_INVALID_STATE, "filesystem is already stopped");
            }

            NProto::TError error;
            try {
                const auto& response = future.GetValue();
                callContext->Error = response.GetError();
                RequestStats->RequestCompleted(Log, *callContext);
                if (HasError(response)) {
                    STORAGE_ERROR("%s failed to create session: %s",
                        LogTag.c_str(),
                        FormatError(response.GetError()).c_str());

                    return response.GetError();
                }
            } catch (const TServiceError& e) {
                error = MakeError(e.GetCode(), TString(e.GetMessage()));
                STORAGE_ERROR("%s failed to alter: %s",
                    LogTag.c_str(),
                    FormatError(error).c_str());
            } catch (...) {
                error = MakeError(E_FAIL, CurrentExceptionMessage());
                STORAGE_ERROR("%s failed to alter: %s",
                    LogTag.c_str(),
                    FormatError(error).c_str());
            }

            return error;
        });
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TFileSystem::Process(TVfsRequestPtr request)
{
    size_t outSize = 0;
    size_t outChunks = 0;

    {
        // sanity check
        auto guard = request->Out.Acquire();
        if (!guard) {
            return MakeFuture<NProto::TError>(MakeError(E_IO, "failed to acquire output"));
        }

        // TODO: we really want to know that there is
        // at least out_header space available, no need
        // to calc precise size and traverse all the buffers
        const auto& sgList = guard.Get();
        outChunks = sgList.size();
        outSize = SgListGetSize(sgList);
    }

    auto guard = request->In.Acquire();
    if (!guard) {
        return MakeFuture<NProto::TError>(MakeError(E_IO, "failed to acquire input"));
    }

    const TSgList& sglist = guard.Get();
    TSgListInputIterator iter(sglist);

    struct fuse_in_header header;
    if (!iter.Read(header)) {
        return MakeFuture<NProto::TError>(MakeError(E_IO, "failed to read header"));
    }

    // CAUTION: handle could be nullptr
    auto* handler = LookupHandler(header.opcode);
    STORAGE_TRACE("%s IN[c:%lu][s:%lu] OUT[c:%lu][s:%lu]; %s -> H: %s",
        LogTag.c_str(),
        sglist.size(),
        SgListGetSize(sglist),
        outChunks,
        outSize,
        Dump(header).c_str(),
        handler ? TString(handler->Name).c_str() : "NOTSUPP");

    auto ctx = std::make_shared<TVfsRequestContext>(
        MakeIntrusive<TCallContext>(Config->GetFileSystemId(), header.unique),
        // request should live till guard is released
        request);

    if (!handler) {
        // unsupported request: either there is room for answer or smth obviously broken
        ReplyError(ctx, -ENOSYS);
        return {};
    } else if (!handler->Oneway && outSize < sizeof(fuse_out_header)) {
        // except for *forget we should be able to send at least out_header
        return MakeFuture(ErrorIO());
    }

    ctx->CallContext->RequestType = handler->RequestType;
    RequestStats->RequestStarted(Log, *ctx->CallContext);

    return (this->*handler->Handler)(std::move(ctx), header, iter);
}

////////////////////////////////////////////////////////////////////////////////

IFileSystemPtr CreateFileSystem(
    NVFS::TVFSConfigPtr config,
    NClient::ISessionPtr session,
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    IRequestStatsRegistryPtr statsRegistry,
    IProfileLogPtr profileLog)
{
    return std::make_shared<TFileSystem>(
        std::move(config),
        std::move(session),
        std::move(logging),
        std::move(scheduler),
        std::move(statsRegistry),
        std::move(profileLog));
}

}   // namespace NCloud::NFileStore::NVFSVhost
