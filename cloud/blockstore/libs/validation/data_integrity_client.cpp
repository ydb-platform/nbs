#include "validation.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
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

#include <google/protobuf/util/message_differencer.h>

#include <contrib/libs/sparsehash/src/sparsehash/dense_hash_map>

namespace NCloud::NBlockStore::NClient {

using namespace NMonitoring;
using namespace NThreading;

using MessageDifferencer = google::protobuf::util::MessageDifferencer;

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<NProto::TChecksum> CalculateChecksumsForWriteRequest(
    const TSgList& sgList,
    size_t firstChecksumLength,
    ui32 blockSize)
{
    TVector<NProto::TChecksum> result;
    firstChecksumLength = Min(firstChecksumLength, sgList.size());
    size_t i = 0;
    TBlockChecksum checksumCalculator;
    for (; i < firstChecksumLength; i++) {
        auto blockData = sgList[i];
        checksumCalculator.Extend(blockData.Data(), blockData.Size());
    }
    NProto::TChecksum checksum;
    checksum.SetChecksum(checksumCalculator.GetValue());
    checksum.SetByteCount(firstChecksumLength * blockSize);
    result.push_back(std::move(checksum));

    if (firstChecksumLength < sgList.size()) {
        NProto::TChecksum checksum;
        checksum.SetByteCount(
            (sgList.size() - firstChecksumLength) * blockSize);
        TBlockChecksum checksumCalculator;
        for (; i < sgList.size(); i++) {
            TVector<ui32> result;
            auto blockData = sgList[i];
            checksumCalculator.Extend(blockData.Data(), blockData.Size());
        }
        checksum.SetChecksum(checksumCalculator.GetValue());
        result.push_back(std::move(checksum));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestCounters
{
    TDynamicCounters::TCounterPtr ReadRequests;
    TDynamicCounters::TCounterPtr WriteRequests;
    TDynamicCounters::TCounterPtr ReadChecksumMismatch;

    void Register(TDynamicCounters& counters)
    {
        ReadRequests = counters.GetCounter("ReadRequests", true);
        WriteRequests = counters.GetCounter("WriteRequests", true);
        ReadChecksumMismatch =
            counters.GetCounter("ReadChecksumMismatch", true);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDataIntegrityClient final: public IBlockStore
{
private:
    mutable TLog Log;

    const IBlockStorePtr Client;
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
        ui32 blockSize)
    : Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    , Client(std::move(client))
    , BlockSize(blockSize)
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", "data_integrity");
    RequestCounters.Register(*Counters);
}

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
            const auto currentChecksum = CalculateChecksum(sgList);
            const auto& checksum = response.GetChecksum();

            STORAGE_INFO("READ; currentChecksum: " << currentChecksum.DebugString()
                << "; incoming checksum: " << checksum.DebugString());
            if (!MessageDifferencer::Equals(checksum, currentChecksum)) {
                RequestCounters.ReadChecksumMismatch->Inc();
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Data integrity violation. Current checksum: "
                        << currentChecksum.DebugString()
                        << "; Incoming checksum: " << checksum.DebugString())};
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
            const auto currentChecksum = CalculateChecksum(sgList);
            const auto& checksum = response.GetChecksum();

            STORAGE_INFO("READ_LOCAL; currentChecksum: " << currentChecksum.DebugString()
                 << "; incoming checksum: " << checksum.DebugString());
            if (!MessageDifferencer::Equals(checksum, currentChecksum)) {
                RequestCounters.ReadChecksumMismatch->Inc();
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Data integrity violation. Current checksum: "
                        << currentChecksum.DebugString()
                        << "; Incoming checksum: " << checksum.DebugString())};
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

    const ui32 maxBlockCount = MaxSubRequestSize / BlockSize;
    // Calculate the point where we should split the checksums calculation
    const ui64 splitChecksumsIndex =
        AlignUp<ui64>(request->GetStartIndex() + 1, maxBlockCount);

    TSgList sgList = GetSgList(*request);
    auto checksums = CalculateChecksumsForWriteRequest(
        sgList,
        splitChecksumsIndex - request->GetStartIndex(),
        BlockSize);
    for (auto& checksum: checksums) {
        *request->AddChecksums() = std::move(checksum);
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

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        MakeFuture<NProto::TWriteBlocksLocalResponse>(TErrorResponse{MakeError(
            E_CANCELLED,
            "failed to acquire sglist in DataIntegrityClient")});
        return true;
    }

    const ui32 maxBlockCount = MaxSubRequestSize / BlockSize;
    // Calculate the point where we should split the checksums calculation
    const ui64 splitChecksumsIndex =
        AlignUp<ui64>(request->GetStartIndex() + 1, maxBlockCount);

    const auto& sgList = guard.Get();
    auto checksums = CalculateChecksumsForWriteRequest(
        sgList,
        splitChecksumsIndex - request->GetStartIndex(),
        BlockSize);
    for (auto& checksum: checksums) {
        *request->AddChecksums() = std::move(checksum);
    }

    response =
        Client->WriteBlocksLocal(std::move(callContext), std::move(request));
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateDataIntegrityClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr client,
    ui32 blockSize)
{
    return std::make_unique<TDataIntegrityClient>(
        std::move(logging),
        std::move(monitoring),
        std::move(client),
        blockSize);
}

}   // namespace NCloud::NBlockStore::NClient
