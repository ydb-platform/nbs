#include "validation.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <contrib/libs/sparsehash/src/sparsehash/dense_hash_map>

namespace NCloud::NBlockStore::NClient {

using namespace NMonitoring;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool IsRequestFailed(const TFuture<T>& future)
{
    if (!future.HasException()) {
        return HasError(future.GetValue());
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

enum class EOperation
{
    Read,
    Write,
    Zero
};

TStringBuf GetOperationString(EOperation op)
{
    switch (op) {
        case EOperation::Read:  return "read";
        case EOperation::Write: return "write";
        case EOperation::Zero:  return "zero";
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TOperationRange
{
    ui64 Begin;
    ui64 End;
    EOperation Op;

    ui32 Size() const
    {
        return SafeIntegerCast<ui32>(End - Begin + 1);
    }

    bool Overlaps(const TOperationRange& other) const
    {
        return Begin <= other.End && other.Begin <= End;
    }

    bool operator ==(const TOperationRange& other) const
    {
        return Begin == other.Begin && End == other.End && Op == other.Op;
    }
};

////////////////////////////////////////////////////////////////////////////////

TVector<ui32> CalculateChecksumsForWriteRequest(
    const TSgList& sgList,
    size_t splitLength)
{
    TVector<ui32> result;
    for (size_t i = 0; i < sgList.size() / splitLength; i++) {
        TBlockChecksum checksum;
        for (size_t j = i * splitLength; j < (i + 1) * splitLength; j++) {
            auto blockData = sgList[j];
            checksum.Extend(blockData.Data(), blockData.Size());
        }
        result.push_back(checksum.GetValue());
    }
    return result;
}

ui32 CalculateChecksum(const TSgList& sgList)
{
    TBlockChecksum checksum;
    for (auto blockData: sgList) {
        checksum.Extend(blockData.Data(), blockData.Size());
    }
    return checksum.GetValue();
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestCounters
{
    TDynamicCounters::TCounterPtr MountedVolumes;

    TDynamicCounters::TCounterPtr MountRequests;
    TDynamicCounters::TCounterPtr UnmountRequests;
    TDynamicCounters::TCounterPtr ReadRequests;
    TDynamicCounters::TCounterPtr WriteRequests;

    TDynamicCounters::TCounterPtr InconsistentRead;
    TDynamicCounters::TCounterPtr ReadWriteRace;
    TDynamicCounters::TCounterPtr WriteWriteRace;

    TDynamicCounters::TCounterPtr BlocksCount;

    void Register(TDynamicCounters& counters)
    {
        MountedVolumes = counters.GetCounter("MountedVolumes");

        MountRequests = counters.GetCounter("MountRequests", true);
        UnmountRequests = counters.GetCounter("UnmountRequests", true);
        ReadRequests = counters.GetCounter("ReadRequests", true);
        WriteRequests = counters.GetCounter("WriteRequests", true);

        InconsistentRead = counters.GetCounter("InconsistentRead", true);
        ReadWriteRace = counters.GetCounter("ReadWriteRace", true);
        WriteWriteRace = counters.GetCounter("WriteWriteRace", true);

        BlocksCount = counters.GetCounter("BlocksCount");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDataIntegrityClient final: public IBlockStore
{
private:
    mutable TLog Log;

    const IBlockStorePtr Client;
    const NProto::EStorageMediaKind StorageMediaKind;
    const ui32 BlockSize;

    TDynamicCountersPtr Counters;
    TRequestCounters RequestCounters;

    // TVolumeMap Volumes;
    // TMutex Lock;

public:
    TDataIntegrityClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        NProto::EStorageMediaKind mediaKind,
        ui32 blockSize);

    void Start() override
    {
        Client->Start();
    }

    void Stop() override
    {
        Client->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Client->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        TFuture<NProto::T##name##Response> response;                           \
        if (!HandleRequest(callContext, request, response)) {                  \
            response = Client->name(                                           \
                std::move(callContext), std::move(request));                   \
        }                                                                      \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        TFuture<NProto::T##name##Response> response;                           \
        if (!HandleRequest(callContext, request, response)) {                  \
            response = Client->name(                                           \
                std::move(callContext), std::move(request));                   \
        }                                                                      \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    // void ReportError(const TString& message) const;

    // void ReportIncompleteRead(
    //     const TVolume& volume,
    //     ui32 requestedCount,
    //     ui32 reportedCount) const;

    // void ReportInconsistentRead(
    //     const TVolume& volume,
    //     const TOperationRange& range,
    //     ui64 blockIndex,
    //     ui64 prevHash,
    //     ui64 newHash) const;

    // void ReportRace(
    //     const TVolume& volume,
    //     const TOperationRange& newRange,
    //     const TOperationRange& oldRange) const;

    // TVolumePtr MountVolume(
    //     const NProto::TVolume& info,
    //     const TString& sessionId);
    // void UnmountVolume(const TString& diskId);
    // TVolumePtr GetVolume(const TString& diskId) const;
    // TMaybe<NProto::TVolume> GetVolumeInfo(const TString& diskId) const;
    // TString GetSessionId(const TString& diskId) const;

    // TMaybe<TOperationRange> CreateRange(
    //     ui64 startIndex,
    //     ui32 blocksCount,
    //     EOperation op);

    // void EnterRange(TVolume& volume, const TOperationRange& range);
    // void LeaveRange(TVolume& volume, const TOperationRange& range);

    // void OnError(TVolume& volume, const TOperationRange& range);

    // TVector<ui64> PrepareRead(
    //     TVolume& volume,
    //     const TOperationRange& range);

    // void CompleteRead(
    //     TVolume& volume,
    //     const TOperationRange& range,
    //     const TVector<ui64>& blocksWritten,
    //     const TVector<ui64>& blocksRead);

    // void PrepareWrite(
    //     TVolume& volume,
    //     const TOperationRange& range);

    // void CompleteWrite(
    //     TVolume& volume,
    //     const TOperationRange& range,
    //     const TVector<ui64>& blocks);

    // void PrepareZero(
    //     TVolume& volume,
    //     const TOperationRange& range);

    // void CompleteZero(
    //     TVolume& volume,
    //     const TOperationRange& range);

    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        TFuture<NProto::TReadBlocksResponse>& response);

    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        TFuture<NProto::TReadBlocksLocalResponse>& response);

    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        TFuture<NProto::TWriteBlocksResponse>& response);

    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        TFuture<NProto::TWriteBlocksLocalResponse>& response);

    // bool HandleRequest(
    //     TCallContextPtr callContext,
    //     std::shared_ptr<NProto::TZeroBlocksRequest> request,
    //     TFuture<NProto::TZeroBlocksResponse>& response);

    template <typename TRequest, typename TResponse>
    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request,
        TFuture<TResponse>& response)
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(response);
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

TDataIntegrityClient::TDataIntegrityClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        NProto::EStorageMediaKind mediaKind,
        ui32 blockSize)
    : Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    , Client(std::move(client))
    , StorageMediaKind(mediaKind)
    , BlockSize(blockSize)
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", "data_integrity");
    RequestCounters.Register(*Counters);
}

////////////////////////////////////////////////////////////////////////////////

// TVolumePtr TDataIntegrityClient::MountVolume(
//     const NProto::TVolume& info,
//     const TString& sessionId)
// {
//     with_lock (Lock) {
//         TVolumeMap::iterator it;
//         bool inserted;

//         std::tie(it, inserted) = Volumes.emplace(
//             info.GetDiskId(),
//             std::make_shared<TVolume>(info, sessionId));

//         if (inserted) {
//             RequestCounters.MountedVolumes->Inc();
//         }

//         return it->second;
//     }
// }

// void TDataIntegrityClient::UnmountVolume(const TString& diskId)
// {
//     with_lock (Lock) {
//         auto it = Volumes.find(diskId);
//         if (it != Volumes.end()) {
//             TVolumePtr volume = it->second;
//             with_lock (volume->Lock) {
//                 RequestCounters.BlocksCount->Sub(volume->Blocks.size());
//             }
//             Volumes.erase(it);
//             RequestCounters.MountedVolumes->Dec();
//         } else {
//             ReportError(TStringBuilder()
//                 << "unknown volume: " << diskId);
//         }
//     }
// }

// TVolumePtr TDataIntegrityClient::GetVolume(const TString& diskId) const
// {
//     with_lock (Lock) {
//         auto it = Volumes.find(diskId);
//         if (it == Volumes.end()) {
//             ReportError(TStringBuilder()
//                 << "unknown volume: " << diskId);
//             return {};
//         }

//         return it->second;
//     }
// }

// TMaybe<NProto::TVolume> TDataIntegrityClient::GetVolumeInfo(
//     const TString& diskId) const
// {
//     with_lock (Lock) {
//         auto it = Volumes.find(diskId);
//         if (it == Volumes.end()) {
//             ReportError(TStringBuilder()
//                 << "unknown volume: " << diskId);
//             return {};
//         }

//         return it->second->Info;
//     }
// }

// TString TDataIntegrityClient::GetSessionId(const TString& diskId) const
// {
//     with_lock (Lock) {
//         auto it = Volumes.find(diskId);
//         if (it == Volumes.end()) {
//             ReportError(TStringBuilder()
//                 << "unknown volume: " << diskId);
//             return {};
//         }

//         return it->second->SessionId;
//     }
// }

////////////////////////////////////////////////////////////////////////////////

// bool TDataIntegrityClient::HandleRequest(
//     TCallContextPtr callContext,
//     std::shared_ptr<NProto::TMountVolumeRequest> request,
//     TFuture<NProto::TMountVolumeResponse>& response)
// {
//     RequestCounters.MountRequests->Inc();

//     response = Client->MountVolume(
//         std::move(callContext), std::move(request)
//     ).Subscribe([this] (const auto& future) {
//         if (!IsRequestFailed(future)) {
//             const auto& response = future.GetValue();
//             MountVolume(
//                 response.GetVolume(),
//                 response.GetSessionId());
//         }
//     });
//     return true;
// }

// bool TDataIntegrityClient::HandleRequest(
//     TCallContextPtr callContext,
//     std::shared_ptr<NProto::TUnmountVolumeRequest> request,
//     TFuture<NProto::TUnmountVolumeResponse>& response)
// {
//     RequestCounters.UnmountRequests->Inc();

//     auto diskId = request->GetDiskId();
//     response = Client->UnmountVolume(
//         std::move(callContext), std::move(request)
//     ).Subscribe([=, this] (const auto& future) {
//         if (!IsRequestFailed(future)) {
//             UnmountVolume(diskId);
//         }
//     });
//     return true;
// }

bool TDataIntegrityClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    TFuture<NProto::TReadBlocksResponse>& response)
{
    RequestCounters.ReadRequests->Inc();

    auto result =
        Client->ReadBlocks(std::move(callContext), std::move(request));

    response = result.Apply(
        [result, this](const auto&) mutable -> NProto::TReadBlocksResponse
        {
            NProto::TReadBlocksResponse response = result.ExtractValue();
            if (HasError(response)) {
                return response;
            }

            if (!response.HasChecksum()) {
                return response;
            }

            auto sgListOrError = GetSgList(response, BlockSize);
            if (HasError(sgListOrError)) {
                return TErrorResponse{sgListOrError.GetError()};
            }

            const auto& sgList = sgListOrError.GetResult();
            const ui32 currentChecksum = CalculateChecksum(sgList);
            const auto& checksum = response.GetChecksum();

            Y_DEBUG_ABORT_UNLESS(checksum.ChecksumsSize() == 1);
            if (checksum.ChecksumsSize() != 1) {
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Invalid checksums amount: "
                                     << checksum.ChecksumsSize())};
            }

            if (checksum.GetChecksums(0) != currentChecksum) {
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Data integrity violation. Current checksum: "
                        << currentChecksum << "; Incoming checksum: "
                        << checksum.GetChecksums(0))};
            }

            return response;
        });

    return true;
}

bool TDataIntegrityClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    TFuture<NProto::TReadBlocksLocalResponse>& response)
{
    RequestCounters.ReadRequests->Inc();

    auto sgList = request->Sglist;
    auto result =
        Client->ReadBlocksLocal(std::move(callContext), std::move(request));

    response = result.Apply(
        [guaredSgList = std::move(sgList), result, this](const auto&) mutable
            -> NProto::TReadBlocksLocalResponse
        {
            NProto::TReadBlocksLocalResponse response = result.ExtractValue();
            if (HasError(response)) {
                return response;
            }

            if (!response.HasChecksum()) {
                return response;
            }

            auto guard = guaredSgList.Acquire();
            if (!guard) {
                return TErrorResponse{MakeError(
                    E_CANCELLED,
                    "failed to acquire sglist in DataIntegrityClient")};
            }

            const auto& sgList = guard.Get();
            const ui32 currentChecksum = CalculateChecksum(sgList);
            const auto& checksum = response.GetChecksum();

            Y_DEBUG_ABORT_UNLESS(checksum.ChecksumsSize() == 1);
            if (checksum.ChecksumsSize() != 1) {
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder() << "Invalid checksums amount: "
                                     << checksum.ChecksumsSize())};
            }

            if (checksum.GetChecksums(0) != currentChecksum) {
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Data integrity violation. Current checksum: "
                        << currentChecksum << "; Incoming checksum: "
                        << checksum.GetChecksums(0))};
            }

            return response;
        });

    return true;
}

bool TDataIntegrityClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    TFuture<NProto::TWriteBlocksResponse>& response)
{
    RequestCounters.WriteRequests->Inc();

    TSgList sgList = GetSgList(*request);
    auto checksums = CalculateChecksumsForWriteRequest(sgList, );
    for (ui32 checksum: checksums) {
        request->MutableChecksum()->AddChecksums(checksum);
    }

    response = Client->WriteBlocks(std::move(callContext), std::move(request));
    return true;
}

bool TDataIntegrityClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    TFuture<NProto::TWriteBlocksLocalResponse>& response)
{
    RequestCounters.WriteRequests->Inc();

    auto volume = GetVolume(request->GetDiskId());
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    EnsureZeroBlockDigestInitialized(*volume);

    auto range = CreateRange(
        request->GetStartIndex(),
        request->BlocksCount,
        EOperation::Write);

    if (!range) {
        // ignore invalid range
        return false;
    }

    auto guard = request->Sglist.Acquire();
    if (guard) {
        auto blocks = CalculateBlocksDigest(
            guard.Get(),
            *DigestCalculator,
            volume->Info.GetBlockSize(),
            range->Begin,
            range->Size(),
            AtomicGet(volume->ZeroBlockDigest)
        );

        PrepareWrite(*volume, *range);

        response = Client->WriteBlocksLocal(
            std::move(callContext), std::move(request)
        ).Subscribe([=, this, blocks_ = std::move(blocks)] (const auto& future) {
            if (!IsRequestFailed(future)) {
                CompleteWrite(*volume, *range, blocks_);
            } else {
                OnError(*volume, *range);
            }
        });
    } else {
        NProto::TWriteBlocksLocalResponse record;
        *record.MutableError() = MakeError(
            E_CANCELLED,
            "failed to acquire sglist in ValidationClient");
        response =
            MakeFuture<NProto::TWriteBlocksLocalResponse>(std::move(record));
    }

    return true;
}

// bool TDataIntegrityClient::HandleRequest(
//     TCallContextPtr callContext,
//     std::shared_ptr<NProto::TZeroBlocksRequest> request,
//     TFuture<NProto::TZeroBlocksResponse>& response)
// {
//     RequestCounters.WriteRequests->Inc();

//     auto volume = GetVolume(request->GetDiskId());
//     if (!volume) {
//         // ignore unknown volume
//         return false;
//     }

//     EnsureZeroBlockDigestInitialized(*volume);

//     auto range = CreateRange(
//         request->GetStartIndex(),
//         request->GetBlocksCount(),
//         EOperation::Zero);

//     if (!range) {
//         // ignore invalid range
//         return false;
//     }

//     PrepareZero(*volume, *range);

//     response = Client->ZeroBlocks(
//         std::move(callContext), std::move(request)
//     ).Subscribe([=, this] (const auto& future) {
//         if (!IsRequestFailed(future)) {
//             CompleteZero(*volume, *range);
//         } else {
//             OnError(*volume, *range);
//         }
//     });
//     return true;
// }

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateDataIntegrityClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr client,
    NProto::EStorageMediaKind mediaKind,
    ui32 blockSize)
{
    return std::make_unique<TDataIntegrityClient>(
        std::move(logging),
        std::move(monitoring),
        std::move(client),
        mediaKind,
        blockSize);
}

}   // namespace NCloud::NBlockStore::NClient

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NCloud::NBlockStore::NClient::TOperationRange>(
    IOutputStream& out,
    const NCloud::NBlockStore::NClient::TOperationRange& range)
{
    out << NCloud::NBlockStore::NClient::GetOperationString(range.Op)
        << "[" << range.Begin << ".." << range.End << "]";
}
