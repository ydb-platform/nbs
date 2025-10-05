#include "validation.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/common/request_checksum_helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore::NClient {

using namespace NMonitoring;
using namespace NThreading;

using MessageDifferencer = google::protobuf::util::MessageDifferencer;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TVector<NProto::TChecksum> CalculateChecksumsForWriteRequest(
    const T& buffers,
    ui64 startIndex,
    ui32 blockSize)
{
    const ui32 maxBlockCount = MaxSubRequestSize / blockSize;
    // Calculate the point where we should split the checksums calculation
    const ui64 splitChecksumsIndex =
        AlignUp<ui64>(startIndex + 1, maxBlockCount);
    const ui64 firstChecksumLength =
        Min(splitChecksumsIndex - startIndex,
            static_cast<ui64>(buffers.size()));

    TVector<NProto::TChecksum> result;
    size_t i = 0;
    TBlockChecksum checksumCalculator;
    for (; i < firstChecksumLength; i++) {
        const auto& blockData = buffers[i];
        checksumCalculator.Extend(blockData.data(), blockData.size());
    }
    NProto::TChecksum checksum;
    checksum.SetChecksum(checksumCalculator.GetValue());
    checksum.SetByteCount(firstChecksumLength * blockSize);
    result.push_back(std::move(checksum));

    if (firstChecksumLength < static_cast<ui64>(buffers.size())) {
        NProto::TChecksum checksum;
        checksum.SetByteCount(
            (buffers.size() - firstChecksumLength) * blockSize);
        TBlockChecksum checksumCalculator;
        for (; i < static_cast<ui64>(buffers.size()); i++) {
            const auto& blockData = buffers[i];
            checksumCalculator.Extend(blockData.data(), blockData.size());
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
    TDynamicCounters::TCounterPtr WriteChecksumMismatch;

    TDynamicCounters::TCounterPtr EndpointsWithCopyingCount;
    TDynamicCounters::TCounterPtr EndpointsWithoutCopyingCount;

    void Register(TDynamicCounters& counters)
    {
        ReadRequests = counters.GetCounter("ReadRequests", /*derivative=*/true);
        WriteRequests =
            counters.GetCounter("WriteRequests", /*derivative=*/true);
        ReadChecksumMismatch =
            counters.GetCounter("ReadChecksumMismatch", /*derivative=*/true);
        WriteChecksumMismatch =
            counters.GetCounter("WriteChecksumMismatch", /*derivative=*/true);

        EndpointsWithCopyingCount = counters.GetCounter(
            "EndpointsWithCopyingCount",
            /*derivative=*/false);
        EndpointsWithoutCopyingCount = counters.GetCounter(
            "EndpointsWithoutCopyingCount",
            /*derivative=*/false);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDataIntegrityClient final
    : public IBlockStore
    , public std::enable_shared_from_this<TDataIntegrityClient>
{
private:
    TLog Log;

    const IBlockStorePtr Client;
    const TString DiskId;
    const ui32 BlockSize;

    // Indicates whether a checksum mismatch has been detected for a given disk.
    std::atomic_flag ChecksumMismatchDetected;

    TDynamicCountersPtr Counters;
    TRequestCounters RequestCounters;

public:
    TDataIntegrityClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        TString diskId,
        ui32 blockSize,
        bool checksumMismatchDetected);

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

    void SetTag();
};

////////////////////////////////////////////////////////////////////////////////

TDataIntegrityClient::TDataIntegrityClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        TString diskId,
        ui32 blockSize,
        bool checksumMismatchDetected)
    : Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    , Client(std::move(client))
    , DiskId(std::move(diskId))
    , BlockSize(blockSize)
    , ChecksumMismatchDetected(checksumMismatchDetected)
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", "service")
                   ->GetSubgroup("subcomponent", "data_integrity");
    RequestCounters.Register(*Counters);

    if (ChecksumMismatchDetected.test()) {
        RequestCounters.EndpointsWithCopyingCount->Inc();
    } else {
        RequestCounters.EndpointsWithoutCopyingCount->Inc();
    }
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
        [result, weakPtr = weak_from_this()](
            const auto&) mutable -> NProto::TReadBlocksResponse
        {
            NProto::TReadBlocksResponse response = result.ExtractValue();
            if (HasError(response)) {
                return response;
            }

            auto self = weakPtr.lock();
            if (!self) {
                return response;
            }

            if (!response.HasChecksum()) {
                return response;
            }

            const auto currentChecksum =
                CalculateChecksum(response.GetBlocks(), self->BlockSize);
            const auto& checksum = response.GetChecksum();

            if (!MessageDifferencer::Equals(checksum, currentChecksum)) {
                self->RequestCounters.ReadChecksumMismatch->Inc();

                ui32 flags = 0;
                SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Data integrity violation. Current checksum: "
                        << currentChecksum.ShortUtf8DebugString()
                        << "; Incoming checksum: "
                        << checksum.ShortUtf8DebugString(),
                    flags)};
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
        [guardedSgList = std::move(sgList), result, weakPtr = weak_from_this()](
            const auto&) mutable -> NProto::TReadBlocksLocalResponse
        {
            NProto::TReadBlocksLocalResponse response = result.ExtractValue();
            if (HasError(response)) {
                return response;
            }

            auto self = weakPtr.lock();
            if (!self) {
                return response;
            }

            if (!response.HasChecksum()) {
                return response;
            }

            auto guard = guardedSgList.Acquire();
            if (!guard) {
                return TErrorResponse{MakeError(
                    E_CANCELLED,
                    "failed to acquire sglist in DataIntegrityClient")};
            }

            const auto& sgList = guard.Get();
            const auto currentChecksum = CalculateChecksum(sgList);
            const auto& checksum = response.GetChecksum();

            if (!MessageDifferencer::Equals(checksum, currentChecksum)) {
                self->RequestCounters.ReadChecksumMismatch->Inc();

                ui32 flags = 0;
                SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
                return TErrorResponse{MakeError(
                    E_REJECTED,
                    TStringBuilder()
                        << "Data integrity violation. Current checksum: "
                        << currentChecksum.ShortUtf8DebugString()
                        << "; Incoming checksum: "
                        << checksum.ShortUtf8DebugString(),
                    flags)};
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

    auto checksums = CalculateChecksumsForWriteRequest(
        request->GetBlocks().GetBuffers(),
        request->GetStartIndex(),
        BlockSize);
    for (auto& checksum: checksums) {
        *request->AddChecksums() = std::move(checksum);
    }

    response =
        Client->WriteBlocks(std::move(callContext), std::move(request))
            .Apply(
                [weakPtr = weak_from_this()](
                    const TFuture<NProto::TWriteBlocksResponse>& response)
                {
                    auto self = weakPtr.lock();
                    if (!self) {
                        return response.GetValue();
                    }

                    const auto& error = response.GetValue().GetError();
                    if (HasError(error) && HasProtoFlag(
                                               error.GetFlags(),
                                               NProto::EF_CHECKSUM_MISMATCH))
                    {
                        self->RequestCounters.WriteChecksumMismatch->Inc();
                    }

                    return response.GetValue();
                });
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
        response = MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse{MakeError(
                E_CANCELLED,
                "failed to acquire sglist in DataIntegrityClient")});
        return true;
    }

    const auto& sgList = guard.Get();
    auto checksums = CalculateChecksumsForWriteRequest(
        sgList,
        request->GetStartIndex(),
        BlockSize);
    for (auto& checksum: checksums) {
        *request->AddChecksums() = std::move(checksum);
    }

    response =
        Client->WriteBlocksLocal(std::move(callContext), std::move(request))
            .Apply(
                [weakPtr = weak_from_this()](
                    const TFuture<NProto::TWriteBlocksLocalResponse>& response)
                {
                    auto self = weakPtr.lock();
                    if (!self) {
                        return response.GetValue();
                    }

                    const auto& error = response.GetValue().GetError();
                    if (HasError(error) && HasProtoFlag(
                                               error.GetFlags(),
                                               NProto::EF_CHECKSUM_MISMATCH))
                    {
                        self->RequestCounters.WriteChecksumMismatch->Inc();
                    }

                    return response.GetValue();
                });
    return true;
}

void TDataIntegrityClient::SetTag() {
    if (ChecksumMismatchDetected.test()) {
        return;
    }
    ChecksumMismatchDetected.test_and_set();

    RequestCounters.EndpointsWithCopyingCount->Inc();
    Y_DEBUG_ABORT_UNLESS(
        RequestCounters.EndpointsWithoutCopyingCount->Val() > 0);
    RequestCounters.EndpointsWithoutCopyingCount->Dec();

    auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());
    auto request = std::make_shared<NProto::TExecuteActionRequest>();
    request->SetAction("modifytags");  // todo const ?

    NPrivateProto::TModifyTagsRequest input;
    input.SetDiskId(DiskId);
    *input.AddTagsToAdd() = DataIntegrityViolationDetectedTagName;
    request->SetInput(NProtobufJson::Proto2Json(input));
    auto future = Client->ExecuteAction(callContext, std::move(request));
    future.Subscribe(
        [future, weakPtr = weak_from_this()](
            const TFuture<NProto::TExecuteActionResponse>&)
        {
            auto self = weakPtr.lock();
            if (!self) {
                return;
            }

            const auto& error = future.GetValue().GetError();
            if (HasError(error)) {
                auto& Log = self->Log;
                STORAGE_ERROR(
                    "[%s] Failed to add tag after data integrity violation: %s",
                    self->DiskId.c_str(),
                    FormatError(error).c_str());

                // The errors here are probably rare and not critical. So it's
                // ok to not retry. Retrying might be bad in case of some
                // SchemeShard malfunctions, as they might induce a large load.
            }
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateDataIntegrityClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr client,
    const NProto::TVolume& volume)
{
    // "IntermediateWriteBufferTagName" indicates that user modfies the buffer
    // during writes. We should also enable copying in this case.
    const bool checksumMismatchDetected =
        volume.GetTags().contains(DataIntegrityViolationDetectedTagName) ||
        volume.GetTags().contains(IntermediateWriteBufferTagName);
    return std::make_unique<TDataIntegrityClient>(
        std::move(logging),
        std::move(monitoring),
        std::move(client),
        volume.GetDiskId(),
        volume.GetBlockSize(),
        checksumMismatchDetected);
}

}   // namespace NCloud::NBlockStore::NClient
