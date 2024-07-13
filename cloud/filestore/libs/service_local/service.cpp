#include "service.h"

#include "config.h"
#include "fs.h"

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
    return TFsPath(l.Reconstruct());
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

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        static TResponse Execute(TLocalFileSystem& fs, const TRequest& request)\
        {                                                                      \
            return fs.name(request);                                           \
        }                                                                      \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

FILESTORE_DECLARE_METHOD(Ping)
FILESTORE_DECLARE_METHOD(PingSession)
FILESTORE_SERVICE_METHODS(FILESTORE_DECLARE_METHOD)
FILESTORE_DATA_METHODS_SYNC(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DECLARE_METHOD(name, ...)                                    \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        static TFuture<TResponse> ExecuteAsync(                                \
            TLocalFileSystem& fs,                                              \
            const TRequest& request)                                           \
        {                                                                      \
            return fs.name##Async(request);                                    \
        }                                                                      \
    };                                                                         \
// FILESTORE_DECLARE_METHOD

FILESTORE_DATA_METHODS_ASYNC(FILESTORE_DECLARE_METHOD)

#undef FILESTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

class TLocalFileStore final
    : public IFileStoreService
{
private:
    const TLocalFileStoreConfigPtr Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const ITaskQueuePtr TaskQueue;
    const TFsPath Root;

    TRWMutex Lock;
    THashMap<TString, TLocalFileSystemPtr> FileSystems;

    TLog Log;

public:
    TLocalFileStore(
            TLocalFileStoreConfigPtr config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            ITaskQueuePtr taskQueue)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , TaskQueue(std::move(taskQueue))
        , Root(Config->GetRootPath())
    {
        if (!Root.Exists()) {
            Root.MkDir(Config->GetDefaultPermissions());
        }
        Log = Logging->CreateLog("NFS_SERVICE");
    }

    void Start() override;
    void Stop() override;

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        return TaskQueue->Execute([this, request = std::move(request)] {       \
            return Execute<T##name##Method>(*request);                         \
        });                                                                    \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

FILESTORE_IMPLEMENT_METHOD(Ping)
FILESTORE_IMPLEMENT_METHOD(PingSession)
FILESTORE_SERVICE_METHODS(FILESTORE_IMPLEMENT_METHOD)
FILESTORE_DATA_METHODS_SYNC(FILESTORE_IMPLEMENT_METHOD)


#undef FILESTORE_IMPLEMENT_METHOD


#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        return TaskQueue->Execute([this, request = std::move(request)] {       \
            return ExecuteAsync<T##name##Method>(*request);                    \
        });                                                                    \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

FILESTORE_DATA_METHODS_ASYNC(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> responseHandler) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(responseHandler);
    }

private:
    template <typename T>
    typename T::TResponse Execute(const typename T::TRequest& request)
    {
        const auto& id = GetFileSystemId(request);
        if (!id) {
            return TErrorResponse(E_ARGUMENT, "invalid file system identifier");
        }

        auto fs = FindFileSystem(id);
        if (!fs) {
            return TErrorResponse(E_ARGUMENT, TStringBuilder()
                << "invalid file system: " << id.Quote());
        }

        try {
            return T::Execute(*fs, request);
        } catch (const TServiceError& e) {
            return TErrorResponse(e.GetCode(), TString(e.GetMessage()));
        } catch (...) {
            return TErrorResponse(E_FAIL, CurrentExceptionMessage());
        }
    }

    template <typename T>
    TFuture<typename T::TResponse> ExecuteAsync(const typename T::TRequest& request)
    {
        const auto& id = GetFileSystemId(request);
        if (!id) {
            return MakeFuture<typename T::TResponse>(
                TErrorResponse(E_ARGUMENT, "invalid file system identifier"));
        }

        auto fs = FindFileSystem(id);
        if (!fs) {
            return MakeFuture<typename T::TResponse>(TErrorResponse(
                E_ARGUMENT,
                TStringBuilder() << "invalid file system: " << id.Quote()));
        }

        try {
            return T::ExecuteAsync(*fs, request);
        } catch (const TServiceError& e) {
            return MakeFuture<typename T::TResponse>(
                TErrorResponse(e.GetCode(), TString(e.GetMessage())));
        } catch (...) {
            return MakeFuture<typename T::TResponse>(
                TErrorResponse(E_FAIL, CurrentExceptionMessage()));
        }
    }

    template <>
    NProto::TPingResponse Execute<TPingMethod>(
        const NProto::TPingRequest& request)
    {
        Y_UNUSED(request);
        return {};
    }

    template <>
    NProto::TCreateFileStoreResponse Execute<TCreateFileStoreMethod>(
        const NProto::TCreateFileStoreRequest& request)
    {
        return CreateFileStore(request);
    }

    template <>
    NProto::TDestroyFileStoreResponse Execute<TDestroyFileStoreMethod>(
        const NProto::TDestroyFileStoreRequest& request)
    {
        return DestroyFileStore(request);
    }

    template <>
    NProto::TAlterFileStoreResponse Execute<TAlterFileStoreMethod>(
        const NProto::TAlterFileStoreRequest& request)
    {
        return AlterFileStore(request);
    }

    template <>
    NProto::TListFileStoresResponse Execute<TListFileStoresMethod>(
        const NProto::TListFileStoresRequest& request)
    {
        return ListFileStores(request);
    }

    template <>
    NProto::TDescribeFileStoreModelResponse Execute<TDescribeFileStoreModelMethod>(
        const NProto::TDescribeFileStoreModelRequest&)
    {
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "DescribeFileStoreModel is not implemented for the local service");
    }

    template <>
    NProto::TResizeFileStoreResponse Execute<TResizeFileStoreMethod>(
        const NProto::TResizeFileStoreRequest&)
    {
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "ResizeFileStore is not implemented for the local service");
    }

    template <>
    NProto::TSubscribeSessionResponse Execute<TSubscribeSessionMethod>(
        const NProto::TSubscribeSessionRequest&)
    {
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "SubscribeSession is not implemented for the local service");
    }

    template <>
    NProto::TGetSessionEventsResponse Execute<TGetSessionEventsMethod>(
        const NProto::TGetSessionEventsRequest&)
    {
        return TErrorResponse(
            E_NOT_IMPLEMENTED,
            "GetSessionEvents is not implemented for the local service");
    }

    template <>
    NProto::TExecuteActionResponse Execute<TExecuteActionMethod>(
        const NProto::TExecuteActionRequest&)
    {
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
        const NProto::TFileStore& store);

    TLocalFileSystemPtr FindFileSystem(const TString& id);
};

////////////////////////////////////////////////////////////////////////////////

void TLocalFileStore::Start()
{
    const auto& prefix = Config->GetPathPrefix();

    TVector<TFsPath> children;
    Root.List(children);

    TWriteGuard guard(Lock);

    for (const auto& child: children) {
        if (child.GetName().StartsWith(prefix) && child.IsDirectory()) {
            auto id = child.GetName().substr(prefix.size());
            auto path = Concat(Root, "." + child.GetName());
            if (!path.Exists()) {
                STORAGE_WARN("skipped suspicious " << id.Quote());
                continue;
            }

            if (FileSystems.contains(id)) {
                continue;
            }

            NProto::TFileStore store;
            LoadFileStoreProto(path, store);
            InitFileSystem(id, child, store);

            STORAGE_INFO("restored local store " << id.Quote());
        }
    }
}

void TLocalFileStore::Stop()
{
    // TODO: flush?
}

NProto::TCreateFileStoreResponse TLocalFileStore::CreateFileStore(
    const NProto::TCreateFileStoreRequest& request)
{
    STORAGE_TRACE("CreateFileStore " << DumpMessage(request));

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

        google::protobuf::util::MessageDifferencer comparator;
        if (!comparator.Equals(store, it->second->GetConfig())) {
            errorCode = E_ARGUMENT;
        }

        return TErrorResponse(errorCode, TStringBuilder()
            << "file store already exists: " << id.Quote());
    }

    const TString& name = Config->GetPathPrefix() + id;

    TFsPath meta = Concat(Config->GetRootPath(), "." + name);
    SaveFileStoreProto(meta, store);

    TFsPath root = Concat(Config->GetRootPath(), name);
    root.MkDir(Config->GetDefaultPermissions());

    InitFileSystem(id, root, store);

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

    TWriteGuard guard(Lock);

    auto it = FileSystems.find(id);
    if (it == FileSystems.end()) {
        return TErrorResponse(S_FALSE, TStringBuilder()
            << "invalid file system: " << id.Quote());
    }

    TFsPath path = Concat(Config->GetRootPath(), Config->GetPathPrefix() + id);
    path.ForceDelete();

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

    TWriteGuard guard(Lock);

    auto it = FileSystems.find(id);
    if (it == FileSystems.end()) {
        return TErrorResponse(E_ARGUMENT, TStringBuilder()
            << "file store doesn't exists: " << id.Quote());
    }

    NProto::TFileStore store = it->second->GetConfig();
    AlterRequestToFileStore(request, store);

    const TString& name = Config->GetPathPrefix() + id;
    TFsPath meta = Concat(Config->GetRootPath(), "." + name);
    SaveFileStoreProto(meta, store);

    it->second->SetConfig(store);

    return {};
}

NProto::TListFileStoresResponse TLocalFileStore::ListFileStores(
    const NProto::TListFileStoresRequest& request)
{
    STORAGE_TRACE("ListFileStores " << DumpMessage(request));
    NProto::TListFileStoresResponse response;

    TReadGuard guard(Lock);
    for (const auto& [id, _]: FileSystems) {
        *response.AddFileStores() = id;
    }

    return response;
}

TLocalFileSystemPtr TLocalFileStore::InitFileSystem(
    const TString& id,
    const TFsPath& root,
    const NProto::TFileStore& store)
{
    auto fs = std::make_shared<TLocalFileSystem>(
        Config,
        store,
        root,
        Timer,
        Scheduler,
        Logging);

    auto [it, inserted] = FileSystems.emplace(id, fs);
    Y_ABORT_UNLESS(inserted);

    return fs;
}

TLocalFileSystemPtr TLocalFileStore::FindFileSystem(const TString& id)
{
    TReadGuard guard(Lock);

    auto it = FileSystems.find(id);
    if (it != FileSystems.end()){
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
    ITaskQueuePtr taskQueue)
{
    return std::make_shared<TLocalFileStore>(
        std::move(config),
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(taskQueue));
}

}   // namespace NCloud::NFileStore
