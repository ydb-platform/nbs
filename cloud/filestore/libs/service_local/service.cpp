#include "service.h"

#include "fs.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/rwlock.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TFsPath Concat(const TFsPath& lhs, const TFsPath& rhs)
{
    TPathSplit l(lhs.GetPath());
    TPathSplit r(rhs.GetPath());

    l.AppendMany(r.begin(), r.end());
    return l.Reconstruct();
}

void CreateRequestToFileStore(
    const NProto::TCreateFileStoreRequest& request,
    NProto::TFileStore& store)
{
    store.SetFileSystemId(GetFileSystemId(request));

    store.SetProjectId(request.GetProjectId());
    store.SetFolderId(request.GetFolderId());
    store.SetCloudId(request.GetCloudId());

    store.SetBlockSize(request.GetBlockSize());
    store.SetBlocksCount(request.GetBlocksCount());
}

void AlterRequestToFileStore(
    const NProto::TAlterFileStoreRequest& request,
    NProto::TFileStore& store)
{
    if (const auto& project = request.GetProjectId()) {
        store.SetProjectId(project);
    }

    if (const auto& cloud = request.GetCloudId()) {
        store.SetCloudId(cloud);
    }

    if (const auto& folder = request.GetFolderId()) {
        store.SetFolderId(folder);
    }
}

void LoadFileStoreProto(const TString& fileName, NProto::TFileStore& store)
{
    TFileInput in(fileName);
    ParseFromTextFormat(in, store);
}

void SaveFileStoreProto(const TString& fileName, const NProto::TFileStore& store)
{
    TFileOutput out(fileName);
    SerializeToTextFormat(store, out);
}

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD_SYNC(name, ...)                               \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
        using TResult = TResponse;                                             \
        static constexpr auto RequestType = EFileStoreRequest::name;           \
                                                                               \
        static TResult Execute(                                                \
            TLocalFileSystem& fs,                                              \
            const TRequest& request,                                           \
            NProto::TProfileLogRequestInfo& logRequest)                        \
        {                                                                      \
            Y_UNUSED(logRequest);                                              \
            return fs.name(request);                                           \
        }                                                                      \
                                                                               \
        static TResult ErrorResponse(ui32 code, TString message)               \
        {                                                                      \
            return NCloud::ErrorResponse<TResponse>(code, std::move(message)); \
        }                                                                      \
    };                                                                         \
// FILESTORE_DECLARE_METHOD_SYNC

#define FILESTORE_DECLARE_METHOD_ASYNC(name, ...)                            \
    struct T##name##Method                                                   \
    {                                                                        \
        using TRequest = NProto::T##name##Request;                           \
        using TResponse = NProto::T##name##Response;                         \
        using TResult = TFuture<TResponse>;                                  \
        static constexpr auto RequestType = EFileStoreRequest::name;         \
                                                                             \
        static TResult Execute(                                              \
            TLocalFileSystem& fs,                                            \
            TRequest& request,                                               \
            NProto::TProfileLogRequestInfo& logRequest)                      \
        {                                                                    \
            return fs.name##Async(request, logRequest);                      \
        }                                                                    \
                                                                             \
        static TResult ErrorResponse(ui32 code, TString message)             \
        {                                                                    \
            return MakeFuture(                                               \
                NCloud::ErrorResponse<TResponse>(code, std::move(message))); \
        }                                                                    \
    };                                                                       \
// FILESTORE_DECLARE_METHOD_SYNC

FILESTORE_SERVICE_LOCAL_SYNC(FILESTORE_DECLARE_METHOD_SYNC)
FILESTORE_SERVICE_LOCAL_ASYNC(FILESTORE_DECLARE_METHOD_ASYNC)

#undef FILESTORE_DECLARE_METHOD_SYNC
#undef FILESTORE_DECLARE_METHOD_ASYNC

    struct TReadDataLocalMethod
    {
        using TRequest = NProto::TReadDataLocalRequest;
        using TResponse = NProto::TReadDataLocalResponse;
        using TResult = TFuture<TResponse>;
        static constexpr auto RequestType = EFileStoreRequest::ReadData;

        static TResult Execute(
            TLocalFileSystem& fs,
            TRequest& request,
            NProto::TProfileLogRequestInfo& logRequest)
        {
            return fs.ReadDataLocalAsync(request, logRequest);
        }

        static TResult ErrorResponse(ui32 code, TString message)
        {
            return MakeFuture(
                NCloud::ErrorResponse<TResponse>(code, std::move(message)));
        }
    };

    struct TWriteDataLocalMethod
    {
        using TRequest = NProto::TWriteDataLocalRequest;
        using TResponse = NProto::TWriteDataLocalResponse;
        using TResult = TFuture<TResponse>;
        static constexpr auto RequestType = EFileStoreRequest::WriteData;

        static TResult Execute(
            TLocalFileSystem& fs,
            TRequest& request,
            NProto::TProfileLogRequestInfo& logRequest)
        {
            return fs.WriteDataLocalAsync(request, logRequest);
        }

        static TResult ErrorResponse(ui32 code, TString message)
        {
            return MakeFuture(
                NCloud::ErrorResponse<TResponse>(code, std::move(message)));
        }
    };

////////////////////////////////////////////////////////////////////////////////

class TLocalFileStore final
    : public IFileStoreService
    , public std::enable_shared_from_this<TLocalFileStore>
{
private:
    const TLocalFileStoreConfigPtr Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IFileIOServicePtr FileIOService;
    const ITaskQueuePtr TaskQueue;
    const IProfileLogPtr ProfileLog;
    const TFsPath RootPath;
    const TFsPath StatePath;

    TRWMutex Lock;
    THashMap<TString, TLocalFileSystemPtr> FileSystems;

    TLog Log;

public:
    TLocalFileStore(
            TLocalFileStoreConfigPtr config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IFileIOServicePtr fileIOService,
            ITaskQueuePtr taskQueue,
            IProfileLogPtr profileLog)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , FileIOService(std::move(fileIOService))
        , TaskQueue(std::move(taskQueue))
        , ProfileLog(std::move(profileLog))
        , RootPath(Config->GetRootPath())
        , StatePath(Config->GetStatePath())
    {
        RootPath.CheckExists();
        Log = Logging->CreateLog("NFS_SERVICE");
    }

    void Start() override;
    void Stop() override;

#define FILESTORE_IMPLEMENT_METHOD_SYNC(name, ...)                             \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        return TaskQueue->Execute([this, request = std::move(request)] {       \
            return ExecuteWithProfileLog<T##name##Method>(*request);           \
        });                                                                    \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD_SYNC

#define FILESTORE_IMPLEMENT_METHOD_ASYNC(name, ...)                            \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        return TaskQueue->Execute([this, request = std::move(request)] {       \
            return ExecuteWithProfileLogAsync<T##name##Method>(*request);      \
        });                                                                    \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD_ASYNC

FILESTORE_SERVICE_LOCAL_SYNC(FILESTORE_IMPLEMENT_METHOD_SYNC)
FILESTORE_SERVICE_LOCAL_ASYNC(FILESTORE_IMPLEMENT_METHOD_ASYNC)
FILESTORE_IMPLEMENT_METHOD_ASYNC(ReadDataLocal)
FILESTORE_IMPLEMENT_METHOD_ASYNC(WriteDataLocal)

#undef FILESTORE_IMPLEMENT_METHOD

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> responseHandler) override
    {
        Y_UNUSED(callContext, request, responseHandler);
    }

private:
    struct TFileStorePaths
    {
        TFsPath MetaPath;
        TFsPath RootPath;
        TFsPath StatePath;
    };

    TFileStorePaths GetPaths(const TString& fsId)
    {
        TFileStorePaths paths;

        const TString& name = Config->GetPathPrefix() + fsId;

        paths.MetaPath = Concat(RootPath, "." + name);
        paths.RootPath = Concat(RootPath, name);
        paths.StatePath = Concat(StatePath, ".state_" + name);
        return paths;
    }

    std::optional<TString> ParseFsIdFromStateDir(const TFsPath& dir)
    {
        const auto& prefix = ".state_" + Config->GetPathPrefix();
        if (dir.GetName().StartsWith(prefix) && dir.IsDirectory()) {
            return dir.GetName().substr(prefix.size());
        }

        return {};
    }

    std::optional<TString> ParseFsIdFromRootDir(const TFsPath& dir)
    {
        const auto& prefix = Config->GetPathPrefix();
        if (dir.GetName().StartsWith(prefix) && dir.IsDirectory()) {
            return dir.GetName().substr(prefix.size());
        }

        return {};
    }

    template <typename T>
    void ProfileLogInit(
        NProto::TProfileLogRequestInfo& logRequest,
        const typename T::TRequest& request)
    {
        logRequest.SetRequestType(static_cast<ui32>(T::RequestType));
        logRequest.SetTimestampMcs(Now().MicroSeconds());
        InitProfileLogRequestInfo(logRequest, request);
    }

    template <typename T>
    void ProfileLogFinalize(
        NProto::TProfileLogRequestInfo&& logRequest,
        const typename T::TResponse& response,
        TString&& fsId)
    {
        FinalizeProfileLogRequestInfo(logRequest, response);
        logRequest.SetDurationMcs(
            Now().MicroSeconds() - logRequest.GetTimestampMcs());
        logRequest.SetErrorCode(response.GetError().GetCode());
        ProfileLog->Write({std::move(fsId), std::move(logRequest)});
    }

    template <typename T>
    void ProfileLogFinalize(
        NProto::TProfileLogRequestInfo&& logRequest,
        TString&& fsId)
    {
        logRequest.SetDurationMcs(
            Now().MicroSeconds() - logRequest.GetTimestampMcs());
        ProfileLog->Write({std::move(fsId), std::move(logRequest)});
    }

    template <typename T>
    typename T::TResult ExecuteWithProfileLog(typename T::TRequest& request)
    {
        NProto::TProfileLogRequestInfo logRequest;
        ProfileLogInit<T>(logRequest, request);

        auto response = Execute<T>(request, logRequest);

        ProfileLogFinalize<T>(
            std::move(logRequest),
            response,
            GetFileSystemId(request));

        return response;
    }

    template <typename T>
    typename T::TResult ExecuteWithProfileLogAsync(typename T::TRequest& request)
    {
        auto logRequest = std::make_shared<NProto::TProfileLogRequestInfo>();
        ProfileLogInit<T>(*logRequest, request);

        return Execute<T>(request, *logRequest)
            .Subscribe(
                [ptr = weak_from_this(),
                 logRequest = std::move(logRequest),
                 fsId = GetFileSystemId(request)](auto&) mutable
                {
                    auto self = ptr.lock();
                    if (!self) {
                        return;
                    }

                    self->ProfileLogFinalize<T>(
                        std::move(*logRequest),
                        std::move(fsId));
                });
    }

    template <typename T>
    typename T::TResult Execute(
        typename T::TRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        if constexpr (std::is_same_v<T, TCreateSessionMethod>) {
            RefreshFileSystems();
        }

        const auto& id = GetFileSystemId(request);
        if (!id) {
            return T::ErrorResponse(E_ARGUMENT, "invalid file system identifier");
        }

        auto fs = FindFileSystem(id);
        if (!fs) {
            return T::ErrorResponse(E_ARGUMENT, TStringBuilder()
                << "local filestore doesn't exist: " << id.Quote());
        }

        try {
            return T::Execute(*fs, request, logRequest);
        } catch (const TServiceError& e) {
            return T::ErrorResponse(e.GetCode(), TString(e.GetMessage()));
        } catch (...) {
            return T::ErrorResponse(E_FAIL, CurrentExceptionMessage());
        }
    }

    template <>
    NProto::TPingResponse Execute<TPingMethod>(
        NProto::TPingRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(request, logRequest);
        return {};
    }

    template <>
    NProto::TToggleServiceStateResponse Execute<TToggleServiceStateMethod>(
        NProto::TToggleServiceStateRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(request, logRequest);
        return {};
    }

    template <>
    NProto::TCreateFileStoreResponse Execute<TCreateFileStoreMethod>(
        NProto::TCreateFileStoreRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(logRequest);
        return CreateFileStore(request);
    }

    template <>
    NProto::TDestroyFileStoreResponse Execute<TDestroyFileStoreMethod>(
        NProto::TDestroyFileStoreRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(logRequest);
        return DestroyFileStore(request);
    }

    template <>
    NProto::TAlterFileStoreResponse Execute<TAlterFileStoreMethod>(
        NProto::TAlterFileStoreRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(logRequest);
        return AlterFileStore(request);
    }

    template <>
    NProto::TListFileStoresResponse Execute<TListFileStoresMethod>(
        NProto::TListFileStoresRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(logRequest);
        return ListFileStores(request);
    }

    template <>
    NProto::TDescribeFileStoreModelResponse
    Execute<TDescribeFileStoreModelMethod>(
        NProto::TDescribeFileStoreModelRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(request, logRequest);
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "DescribeFileStoreModel is not implemented for the local service");
    }

    template <>
    NProto::TResizeFileStoreResponse Execute<TResizeFileStoreMethod>(
        NProto::TResizeFileStoreRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(request, logRequest);
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "ResizeFileStore is not implemented for the local service");
    }

    template <>
    NProto::TSubscribeSessionResponse Execute<TSubscribeSessionMethod>(
        NProto::TSubscribeSessionRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(request, logRequest);
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "SubscribeSession is not implemented for the local service");
    }

    template <>
    NProto::TGetSessionEventsResponse Execute<TGetSessionEventsMethod>(
        NProto::TGetSessionEventsRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(request, logRequest);
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "GetSessionEvents is not implemented for the local service");
    }

    template <>
    NProto::TExecuteActionResponse Execute<TExecuteActionMethod>(
        NProto::TExecuteActionRequest& request,
        NProto::TProfileLogRequestInfo& logRequest)
    {
        Y_UNUSED(request, logRequest);
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "ExecuteAction is not implemented for the local service");
    }

    NProto::TCreateFileStoreResponse CreateFileStore(
        const NProto::TCreateFileStoreRequest& request);

    NProto::TDestroyFileStoreResponse DestroyFileStore(
        const NProto::TDestroyFileStoreRequest& request);

    NProto::TAlterFileStoreResponse AlterFileStore(
        const NProto::TAlterFileStoreRequest& request);

    NProto::TListFileStoresResponse ListFileStores(
        const NProto::TListFileStoresRequest& request);

    NProto::TDescribeFileStoreModelResponse DescribeFileStoreModel(
        const NProto::TDescribeFileStoreModelRequest& request);

    TLocalFileSystemPtr InitFileSystem(
        const TString& id,
        const TFsPath& root,
        const TFsPath& statePath,
        const NProto::TFileStore& store);

    void RefreshFileSystems();
    TLocalFileSystemPtr LoadFileSystemNoLock(const TString& id);

    TLocalFileSystemPtr FindFileSystem(const TString& id);
};

////////////////////////////////////////////////////////////////////////////////

void TLocalFileStore::Start()
{
    RefreshFileSystems();
}

void TLocalFileStore::Stop()
{
    // TODO: flush?
}

NProto::TCreateFileStoreResponse TLocalFileStore::CreateFileStore(
    const NProto::TCreateFileStoreRequest& request)
{
    STORAGE_INFO("CreateFileStore " << DumpMessage(request));

    const auto& id = GetFileSystemId(request);
    if (!id) {
        return TErrorResponse(E_ARGUMENT, "invalid file system identifier");
    }

    TWriteGuard guard(Lock);

    NProto::TFileStore store;
    CreateRequestToFileStore(request, store);

    auto it = FileSystems.find(id);
    if (it != FileSystems.end()) {
        ui32 errorCode = S_ALREADY;

        if (!google::protobuf::util::MessageDifferencer::Equals(
                store,
                it->second->GetConfig()))
        {
            errorCode = E_ARGUMENT;
        }

        return TErrorResponse(errorCode, TStringBuilder()
            << "local filestore already exists: " << id.Quote());
    }

    auto fsPaths = GetPaths(id);

    SaveFileStoreProto(fsPaths.MetaPath, store);

    fsPaths.RootPath.MkDir(static_cast<int>(Config->GetDefaultPermissions()));
    fsPaths.StatePath.MkDir(static_cast<int>(Config->GetDefaultPermissions()));

    InitFileSystem(id, fsPaths.RootPath, fsPaths.StatePath, store);

    NProto::TCreateFileStoreResponse response;
    response.MutableFileStore()->Swap(&store);

    return response;
}

NProto::TDestroyFileStoreResponse TLocalFileStore::DestroyFileStore(
    const NProto::TDestroyFileStoreRequest& request)
{
    STORAGE_TRACE("DestroyFileStore " << DumpMessage(request));

    const auto& id = GetFileSystemId(request);
    if (!id) {
        return TErrorResponse(E_ARGUMENT, "invalid file system identifier");
    }

    RefreshFileSystems();

    TWriteGuard guard(Lock);

    auto it = FileSystems.find(id);
    if (it == FileSystems.end()) {
        return TErrorResponse(S_FALSE, TStringBuilder()
            << "local filestore doesn't exist: " << id.Quote());
    }

    auto fsPaths = GetPaths(id);
    fsPaths.MetaPath.ForceDelete();
    fsPaths.RootPath.ForceDelete();
    fsPaths.StatePath.ForceDelete();

    FileSystems.erase(it);

    return {};
}

NProto::TAlterFileStoreResponse TLocalFileStore::AlterFileStore(
    const NProto::TAlterFileStoreRequest& request)
{
    STORAGE_TRACE("AlterFileStore " << DumpMessage(request));

    const auto& id = GetFileSystemId(request);
    if (!id) {
        return TErrorResponse(E_ARGUMENT, "invalid file system identifier");
    }

    RefreshFileSystems();

    TWriteGuard guard(Lock);

    auto it = FileSystems.find(id);
    if (it == FileSystems.end()) {
        return TErrorResponse(E_ARGUMENT, TStringBuilder()
            << "local filestore doesn't exist: " << id.Quote());
    }

    NProto::TFileStore store = it->second->GetConfig();
    AlterRequestToFileStore(request, store);

    auto fsPaths = GetPaths(id);

    SaveFileStoreProto(fsPaths.MetaPath, store);

    it->second->SetConfig(store);

    return {};
}

NProto::TListFileStoresResponse TLocalFileStore::ListFileStores(
    const NProto::TListFileStoresRequest& request)
{
    STORAGE_TRACE("ListFileStores " << DumpMessage(request));
    NProto::TListFileStoresResponse response;

    RefreshFileSystems();

    TReadGuard guard(Lock);
    for (const auto& [id, _]: FileSystems) {
        *response.AddFileStores() = id;
    }

    return response;
}

TLocalFileSystemPtr TLocalFileStore::InitFileSystem(
    const TString& id,
    const TFsPath& root,
    const TFsPath& statePath,
    const NProto::TFileStore& store)
{
    auto fs = std::make_shared<TLocalFileSystem>(
        Config,
        store,
        root,
        statePath,
        Timer,
        Scheduler,
        Logging,
        FileIOService);

    auto [it, inserted] = FileSystems.emplace(id, fs);
    Y_DEBUG_ABORT_UNLESS(inserted);

    return fs;
}

void TLocalFileStore::RefreshFileSystems()
{
    TWriteGuard guard(Lock);
    const auto& prefix = Config->GetPathPrefix();

    TVector<TFsPath> children;
    RootPath.List(children);

    // Load new filesystems
    for (const auto& child: children) {
        auto fsId = ParseFsIdFromRootDir(child);
        if (fsId) {
            auto it = FileSystems.find(*fsId);
            if (it != FileSystems.end()) {
                continue;
            }

            LoadFileSystemNoLock(*fsId);
        }
    }

    // Clean filesystems that were destroyed externally
    TVector<TString> destroyedFsIds;
    for (auto& [fsId, _]: FileSystems) {
        auto fsPaths = GetPaths(fsId);
        if (!fsPaths.RootPath.Exists() && !fsPaths.MetaPath.Exists()) {
            destroyedFsIds.push_back(fsId);
        }
    }

    for (auto& fsId: destroyedFsIds) {
        STORAGE_INFO("Unloading local filestore " << fsId.Quote());
        FileSystems.erase(fsId);

        auto fsPaths = GetPaths(fsId);
        fsPaths.StatePath.ForceDelete();
    }

    // Clean local state directories that not used anymore
    // This can happend if filestore-vhost was stopped, filesystem destroyed
    // externally and then filestore-vhost was started
    children.clear();
    StatePath.List(children);
    for (const auto& child: children) {
        auto fsId = ParseFsIdFromStateDir(child);
        if (fsId) {
            if (FileSystems.find(*fsId) != FileSystems.end()) {
                continue;
            }

            STORAGE_INFO("Removing local filestore state " << fsId->Quote());
            child.ForceDelete();
        }
    }
}

TLocalFileSystemPtr TLocalFileStore::LoadFileSystemNoLock(const TString& id)
{
    auto fsPaths = GetPaths(id);
    if (!fsPaths.MetaPath.Exists()) {
        return nullptr;
    }

    TLocalFileSystemPtr fs = nullptr;
    try {
        STORAGE_INFO("Loading local filestore " << id.Quote());

        fsPaths.StatePath.MkDir(
            static_cast<int>(Config->GetDefaultPermissions()));

        NProto::TFileStore store;
        LoadFileStoreProto(fsPaths.MetaPath, store);

        fs = InitFileSystem(id, fsPaths.RootPath, fsPaths.StatePath, store);
        STORAGE_INFO("Loaded local filestore " << id.Quote());
    } catch (...) {
        STORAGE_ERROR("Failed to load fs proto" << CurrentExceptionMessage());
    }

    return fs;
}

TLocalFileSystemPtr TLocalFileStore::FindFileSystem(const TString& id)
{
    TReadGuard guard(Lock);

    auto it = FileSystems.find(id);
    if (it != FileSystems.end()) {
        return it->second;
    }

    return nullptr;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateLocalFileStore(
    TLocalFileStoreConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IFileIOServicePtr fileIOService,
    ITaskQueuePtr taskQueue,
    IProfileLogPtr profileLog)
{
    if (!profileLog) {
        profileLog = CreateProfileLogStub();
    }

    return std::make_shared<TLocalFileStore>(
        std::move(config),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(fileIOService),
        std::move(taskQueue),
        std::move(profileLog));
}

}   // namespace NCloud::NFileStore
