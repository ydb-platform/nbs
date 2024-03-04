#include "service_local.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/discovery/discovery.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/folder/filelist.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NServer {

using namespace NDiscovery;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse, typename T>
TFuture<TResponse> SafeAsyncExecute(T&& block)
{
    try {
        return block();
    } catch (const TServiceError& e) {
        return MakeFuture<TResponse>(
            TErrorResponse(e.GetCode(), e.what()));
    } catch (const TIoSystemError& e) {
        return MakeFuture<TResponse>(
            TErrorResponse(MAKE_SYSTEM_ERROR(e.Status()), e.what()));
    } catch (...) {
        return MakeFuture<TResponse>(
            TErrorResponse(E_FAIL, CurrentExceptionMessage()));
    }
}

template <typename TRequest>
TString GetClientId(const TRequest& request)
{
    if (request.HasHeaders()) {
        return request.GetHeaders().GetClientId();
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

struct TMountSession
{
    const TString SessionId;
    const TString ClientId;
    const IStoragePtr Storage;
    const TStorageAdapter StorageAdapter;

    TMountSession(TString clientId, IStoragePtr storage, ui32 blockSize)
        : SessionId(CreateGuidAsString())
        , ClientId(std::move(clientId))
        , Storage(storage)
        , StorageAdapter(
            std::move(storage),
            blockSize,
            true)
    {}
};

using TMountSessionPtr = std::shared_ptr<TMountSession>;
using TMountSessionMap = THashMap<TString, TMountSessionPtr>;

////////////////////////////////////////////////////////////////////////////////

struct TMountedVolume
{
    const NProto::TVolume Volume;

    TMountSessionMap Sessions;
    TMutex SessionLock;

    TMountedVolume(NProto::TVolume volume)
        : Volume(std::move(volume))
    {}

    TMountSessionPtr CreateSession(TString clientId, IStoragePtr storage)
    {
        with_lock (SessionLock) {
            auto session = std::make_shared<TMountSession>(
                std::move(clientId),
                std::move(storage),
                Volume.GetBlockSize());

            Sessions.emplace(session->SessionId, session);
            return session;
        }
    }

    TMountSessionPtr FindSession(const TString& sessionId)
    {
        with_lock (SessionLock) {
            auto it = Sessions.find(sessionId);
            if (it != Sessions.end()) {
                return it->second;
            }
            return nullptr;
        }
    }

    bool RemoveSession(const TString& sessionId)
    {
        with_lock (SessionLock) {
            auto it = Sessions.find(sessionId);
            if (it != Sessions.end()) {
                Sessions.erase(it);
                return true;
            }
            return false;
        }
    }
};

using TMountedVolumePtr = std::shared_ptr<TMountedVolume>;
using TMountedVolumeMap = THashMap<TString, TMountedVolumePtr>;

////////////////////////////////////////////////////////////////////////////////

const TStringBuf MetaExt = ".meta";
const TStringBuf DataExt = ".data";

////////////////////////////////////////////////////////////////////////////////

class TVolumeManager
{
private:
    const TString DataDir;
    const IStorageProviderPtr StorageProvider;

    TMountedVolumeMap MountedVolumes;
    TMutex MountLock;

public:
    TVolumeManager(const TString& dataDir, IStorageProviderPtr storageProvider)
        : DataDir(dataDir ? dataDir : NFs::CurrentWorkingDirectory())
        , StorageProvider(std::move(storageProvider))
    {}

    void CreateVolume(const TString& diskId, ui32 blockSize, ui64 blocksCount)
    {
        NProto::TVolume volume;
        volume.SetDiskId(diskId);
        volume.SetBlockSize(blockSize);
        volume.SetBlocksCount(blocksCount);

        auto metaPath = MakeMetaPath(diskId);
        auto dataPath = MakeDataPath(diskId);

        if (NFs::Exists(metaPath) && NFs::Exists(dataPath)) {
            TFile fileMeta(metaPath, EOpenModeFlag::OpenExisting);
            TFileInput in(fileMeta);

            NProto::TVolume existedVolume;
            ParseFromTextFormat(in, existedVolume);
            if (volume.GetDiskId() != existedVolume.GetDiskId() ||
                volume.GetBlockSize() != existedVolume.GetBlockSize() ||
                volume.GetBlocksCount() != existedVolume.GetBlocksCount())
            {
                ythrow TServiceError(E_ARGUMENT)
                    << "Volume has already been created with other args";
            }

            // volume already exists
            return;
        }

        TFile fileMeta(metaPath, EOpenModeFlag::CreateAlways);
        TFileOutput out(fileMeta);
        SerializeToTextFormat(volume, out);

        TFile fileData(dataPath, EOpenModeFlag::CreateAlways);
        fileData.Resize(blockSize * blocksCount);
    }

    void ResizeVolume(const TString& diskId, ui64 blocksCount)
    {
        auto volume = DescribeVolume(diskId);
        volume.SetBlocksCount(blocksCount);

        TFile fileMeta(MakeMetaPath(diskId), EOpenModeFlag::CreateAlways);
        TFileOutput out(fileMeta);
        SerializeToTextFormat(volume, out);

        TFile fileData(MakeDataPath(diskId), EOpenModeFlag::OpenExisting);
        fileData.Resize(volume.GetBlockSize() * blocksCount);
    }

    void DestroyVolume(const TString& diskId)
    {
        NFs::Remove(MakeMetaPath(diskId));
        NFs::Remove(MakeDataPath(diskId));
    }

    NProto::TListVolumesResponse ListVolumes()
    {
        NProto::TListVolumesResponse response;

        TFileList fList;
        fList.Fill(DataDir, "", MetaExt, 1);

        while (TStringBuf file = fList.Next()) {
            auto volume = file.Chop(MetaExt.size());

            if (NFs::Exists(MakeDataPath(volume))) {
                *response.MutableVolumes()->Add() = volume;
            }
        }

        return response;
    }

    NProto::TVolume DescribeVolume(const TString& diskId)
    {
        TFile fileMeta(MakeMetaPath(diskId), EOpenModeFlag::OpenExisting);
        TFileInput in(fileMeta);

        NProto::TVolume volume;
        ParseFromTextFormat(in, volume);

        return volume;
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        const TString& diskId,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode)
    {
        // TODO: validate mount options
        Y_UNUSED(mountMode);

        auto mountedVolume = EnsureVolumeMounted(diskId);

        auto dataPath = MakeDataPath(mountedVolume->Volume.GetDiskId());

        NProto::TVolume volume = mountedVolume->Volume;
        volume.SetDiskId(dataPath);
        return StorageProvider->CreateStorage(volume, clientId, accessMode)
            .Apply([=] (const auto& future) {
                auto storage = future.GetValue();
                if (!storage) {
                    NProto::TMountVolumeResponse response;
                    auto& error = *response.MutableError();
                    error.SetCode(E_FAIL);
                    error.SetMessage("Failed to create storage");
                    return response;
                }

                auto session = mountedVolume->CreateSession(
                    clientId,
                    std::move(storage));

                NProto::TMountVolumeResponse response;
                response.SetSessionId(session->SessionId);
                *response.MutableVolume() = mountedVolume->Volume;
                return response;
            });
    }

    void UnmountVolume(const TString& diskId, const TString& sessionId)
    {
        auto volume = FindMountedVolume(diskId);
        if (!volume) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        if (!volume->RemoveSession(sessionId)) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }
    }

    TMountedVolumePtr EnsureVolumeMounted(const TString& diskId)
    {
        with_lock (MountLock) {
            TMountedVolumeMap::insert_ctx ctx;
            auto it = MountedVolumes.find(diskId, ctx);
            if (it == MountedVolumes.end()) {
                auto volume = DescribeVolume(diskId);
                it = MountedVolumes.emplace_direct(
                    ctx,
                    diskId,
                    std::make_shared<TMountedVolume>(std::move(volume)));
            }
            return it->second;
        }
    }

    TMountedVolumePtr FindMountedVolume(const TString& diskId)
    {
        with_lock (MountLock) {
            auto it = MountedVolumes.find(diskId);
            if (it != MountedVolumes.end()) {
                return it->second;
            }
            return nullptr;
        }
    }

private:
    TString MakeMetaPath(TStringBuf diskId) const
    {
        return DataDir + "/" + diskId + MetaExt;
    }

    TString MakeDataPath(TStringBuf diskId) const
    {
        return DataDir + "/" + diskId + DataExt;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TLocalServiceBase
    : public IBlockStore
{
#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED,                                                 \
            "Method " #name " not implemeted"));                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TLocalService final
    : public TLocalServiceBase
{
private:
    IDiscoveryServicePtr DiscoveryService;
    TVolumeManager VolumeManager;

public:
    TLocalService(
            const NProto::TLocalServiceConfig& config,
            IDiscoveryServicePtr discoveryService,
            IStorageProviderPtr storageProvider)
        : DiscoveryService(std::move(discoveryService))
        , VolumeManager(config.GetDataDir(), std::move(storageProvider))
    {}

    void Start() override {}
    void Stop() override {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    TFuture<NProto::TPingResponse> Ping(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TPingRequest> request) override;

    TFuture<NProto::TCreateVolumeResponse> CreateVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TCreateVolumeRequest> request) override;

    TFuture<NProto::TResizeVolumeResponse> ResizeVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TResizeVolumeRequest> request) override;

    TFuture<NProto::TDestroyVolumeResponse> DestroyVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDestroyVolumeRequest> request) override;

    TFuture<NProto::TListVolumesResponse> ListVolumes(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListVolumesRequest> request) override;

    TFuture<NProto::TDescribeVolumeResponse> DescribeVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDescribeVolumeRequest> request) override;

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override;

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override;

    TFuture<NProto::TReadBlocksResponse> ReadBlocks(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TReadBlocksRequest> request) override;

    TFuture<NProto::TWriteBlocksResponse> WriteBlocks(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TWriteBlocksRequest> request) override;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TDiscoverInstancesResponse> DiscoverInstances(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDiscoverInstancesRequest> request) override;

    TFuture<NProto::TAssignVolumeResponse> AssignVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TAssignVolumeRequest> request) override;

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        Y_UNUSED(request);                                                     \
        return MakeFuture(NProto::T##name##Response());                        \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_IMPLEMENT_METHOD(CreateCheckpoint)
    BLOCKSTORE_IMPLEMENT_METHOD(DeleteCheckpoint)
    BLOCKSTORE_IMPLEMENT_METHOD(GetCheckpointStatus)
    BLOCKSTORE_IMPLEMENT_METHOD(UploadClientMetrics)
    BLOCKSTORE_IMPLEMENT_METHOD(QueryAvailableStorage)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TPingResponse> TLocalService::Ping(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TPingRequest> request)
{
    Y_UNUSED(ctx);
    Y_UNUSED(request);
    return MakeFuture<NProto::TPingResponse>();
}

TFuture<NProto::TCreateVolumeResponse> TLocalService::CreateVolume(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TCreateVolumeRequest> request)
{
    Y_UNUSED(ctx);
    return SafeAsyncExecute<NProto::TCreateVolumeResponse>([=] {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        ui32 blockSize = request->GetBlockSize();
        if (!blockSize) {
            blockSize = DefaultBlockSize;
        }

        if (!IsPowerOf2(blockSize)) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume block size is not a power of 2";
        }

        ui64 blocksCount = request->GetBlocksCount();
        if (!blocksCount) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume blocks count should not be zero";
        }

        VolumeManager.CreateVolume(diskId, blockSize, blocksCount);

        return MakeFuture<NProto::TCreateVolumeResponse>();
    });
}

TFuture<NProto::TResizeVolumeResponse> TLocalService::ResizeVolume(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TResizeVolumeRequest> request)
{
    Y_UNUSED(ctx);
    return SafeAsyncExecute<NProto::TResizeVolumeResponse>([=] {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        ui64 blocksCount = request->GetBlocksCount();
        if (!blocksCount) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume blocks count should not be zero";
        }

        VolumeManager.ResizeVolume(diskId, blocksCount);

        return MakeFuture<NProto::TResizeVolumeResponse>();
    });
}

TFuture<NProto::TDestroyVolumeResponse> TLocalService::DestroyVolume(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TDestroyVolumeRequest> request)
{
    Y_UNUSED(ctx);
    return SafeAsyncExecute<NProto::TDestroyVolumeResponse>([=] {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        VolumeManager.DestroyVolume(diskId);

        return MakeFuture<NProto::TDestroyVolumeResponse>();
    });
}

TFuture<NProto::TListVolumesResponse> TLocalService::ListVolumes(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListVolumesRequest> request)
{
    Y_UNUSED(ctx);
    Y_UNUSED(request);
    return SafeAsyncExecute<NProto::TListVolumesResponse>([=] {
        auto response = VolumeManager.ListVolumes();
        return MakeFuture(std::move(response));
    });
}

TFuture<NProto::TDescribeVolumeResponse> TLocalService::DescribeVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDescribeVolumeRequest> request)
{
    Y_UNUSED(ctx);
    return SafeAsyncExecute<NProto::TDescribeVolumeResponse>([=] {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        NProto::TDescribeVolumeResponse response;
        *response.MutableVolume() = VolumeManager.DescribeVolume(diskId);
        return MakeFuture(response);
    });
}

TFuture<NProto::TMountVolumeResponse> TLocalService::MountVolume(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TMountVolumeRequest> request)
{
    Y_UNUSED(ctx);
    return SafeAsyncExecute<NProto::TMountVolumeResponse>([=] {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        const auto& clientId = GetClientId(*request);
        if (!clientId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Client ID should be specified";
        }

        return VolumeManager.MountVolume(
            diskId,
            clientId,
            request->GetVolumeAccessMode(),
            request->GetVolumeMountMode());
    });
}

TFuture<NProto::TUnmountVolumeResponse> TLocalService::UnmountVolume(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TUnmountVolumeRequest> request)
{
    Y_UNUSED(ctx);
    return SafeAsyncExecute<NProto::TUnmountVolumeResponse>([=] {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        const auto& sessionId = request->GetSessionId();
        if (!sessionId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Session ID should be specified";
        }

        VolumeManager.UnmountVolume(diskId, sessionId);

        return MakeFuture<NProto::TUnmountVolumeResponse>();
    });
}

TFuture<NProto::TReadBlocksResponse> TLocalService::ReadBlocks(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TReadBlocksRequest> request)
{
    return SafeAsyncExecute<NProto::TReadBlocksResponse>([=] () mutable {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        const auto& sessionId = request->GetSessionId();
        if (!sessionId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Session ID should be specified";
        }

        auto volume = VolumeManager.FindMountedVolume(diskId);
        if (!volume) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        auto session = volume->FindSession(sessionId);
        if (!session) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        ui64 startIndex = request->GetStartIndex();
        if (startIndex >= volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds read request";
        }

        ui32 blocksCount = request->GetBlocksCount();
        if (startIndex + blocksCount > volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds read request";
        }

        return session->StorageAdapter.ReadBlocks(
            Now(),
            std::move(ctx),
            std::move(request),
            volume->Volume.GetBlockSize());
    });
}

TFuture<NProto::TWriteBlocksResponse> TLocalService::WriteBlocks(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TWriteBlocksRequest> request)
{
    return SafeAsyncExecute<NProto::TWriteBlocksResponse>([=] () mutable {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        const auto& sessionId = request->GetSessionId();
        if (!sessionId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Session ID should be specified";
        }

        auto volume = VolumeManager.FindMountedVolume(diskId);
        if (!volume) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        auto session = volume->FindSession(sessionId);
        if (!session) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        ui64 startIndex = request->GetStartIndex();
        if (startIndex >= volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds write request";
        }

        auto sgListOrError = SgListNormalize(
            GetSgList(*request),
            volume->Volume.GetBlockSize());
        if (HasError(sgListOrError)) {
            const auto& error = sgListOrError.GetError();
            ythrow TServiceError(error.GetCode()) << error.GetMessage();
        }
        auto sglist = sgListOrError.ExtractResult();

        if (startIndex + sglist.size() > volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds write request";
        }

        return session->StorageAdapter.WriteBlocks(
            Now(),
            std::move(ctx),
            std::move(request),
            volume->Volume.GetBlockSize());
    });
}

TFuture<NProto::TZeroBlocksResponse> TLocalService::ZeroBlocks(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    return SafeAsyncExecute<NProto::TZeroBlocksResponse>([=] () mutable {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        const auto& sessionId = request->GetSessionId();
        if (!sessionId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Session ID should be specified";
        }

        auto volume = VolumeManager.FindMountedVolume(diskId);
        if (!volume) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        auto session = volume->FindSession(sessionId);
        if (!session) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        ui64 startIndex = request->GetStartIndex();
        if (startIndex >= volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds write request";
        }

        ui32 blocksCount = request->GetBlocksCount();
        if (startIndex + blocksCount > volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds write request";
        }

        return session->StorageAdapter.ZeroBlocks(
            Now(),
            std::move(ctx),
            std::move(request),
            volume->Volume.GetBlockSize());
    });
}

TFuture<NProto::TReadBlocksLocalResponse> TLocalService::ReadBlocksLocal(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    return SafeAsyncExecute<NProto::TReadBlocksLocalResponse>([=] () mutable {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        const auto& sessionId = request->GetSessionId();
        if (!sessionId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Session ID should be specified";
        }

        auto volume = VolumeManager.FindMountedVolume(diskId);
        if (!volume) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        auto session = volume->FindSession(sessionId);
        if (!session) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        ui64 startIndex = request->GetStartIndex();
        if (startIndex >= volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds read request";
        }

        ui32 blocksCount = request->GetBlocksCount();
        if (startIndex + blocksCount > volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds read request";
        }

        return session->Storage->ReadBlocksLocal(
            std::move(ctx),
            std::move(request));
    });
}

TFuture<NProto::TWriteBlocksLocalResponse> TLocalService::WriteBlocksLocal(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    return SafeAsyncExecute<NProto::TWriteBlocksLocalResponse>([=] () mutable {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        const auto& sessionId = request->GetSessionId();
        if (!sessionId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Session ID should be specified";
        }

        auto volume = VolumeManager.FindMountedVolume(diskId);
        if (!volume) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        auto session = volume->FindSession(sessionId);
        if (!session) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume not mounted";
        }

        ui64 startIndex = request->GetStartIndex();
        if (startIndex >= volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds write request";
        }

        ui32 blocksCount = request->BlocksCount;
        if (startIndex + blocksCount > volume->Volume.GetBlocksCount()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Out of bounds write request";
        }

        return session->Storage->WriteBlocksLocal(
            std::move(ctx),
            std::move(request));
    });
}

TFuture<NProto::TDiscoverInstancesResponse> TLocalService::DiscoverInstances(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TDiscoverInstancesRequest> request)
{
    Y_UNUSED(ctx);

    NProto::TDiscoverInstancesResponse response;
    DiscoveryService->ServeRequest(*request, &response);

    return MakeFuture(std::move(response));
}

TFuture<NProto::TAssignVolumeResponse> TLocalService::AssignVolume(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TAssignVolumeRequest> request)
{
    Y_UNUSED(ctx);
    return SafeAsyncExecute<NProto::TAssignVolumeResponse>([=] {
        const auto& diskId = request->GetDiskId();
        if (!diskId) {
            ythrow TServiceError(E_ARGUMENT)
                << "Volume ID should be specified";
        }

        NProto::TAssignVolumeResponse response;
        *response.MutableVolume() = VolumeManager.DescribeVolume(diskId);
        return MakeFuture(response);
    });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateLocalService(
    const NProto::TLocalServiceConfig& config,
    IDiscoveryServicePtr discoveryService,
    IStorageProviderPtr storageProvider)
{
    return std::make_shared<TLocalService>(
        config,
        std::move(discoveryService),
        std::move(storageProvider));
}

}   // namespace NCloud::NBlockStore::NServer
