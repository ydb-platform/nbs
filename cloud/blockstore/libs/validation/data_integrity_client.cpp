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

#include <cstddef>

namespace NCloud::NBlockStore::NClient {

using namespace NMonitoring;
using namespace NThreading;

using MessageDifferencer = google::protobuf::util::MessageDifferencer;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
[[nodiscard]] TVector<NProto::TChecksum> CalculateChecksumsForWriteRequest(
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

struct TStatCounters
{
    struct TRequestCounters
    {
        TDynamicCounters::TCounterPtr Count;
        TDynamicCounters::TCounterPtr Mismatches;

        void Register(TDynamicCounters& counters)
        {
            Count = counters.GetCounter("Count", /*derivative=*/true);
            Mismatches = counters.GetCounter("Mismatches", /*derivative=*/true);
        }
    };

    struct TIntegrityCounters
    {
        TRequestCounters ReadBlocksLocal;
        TRequestCounters WriteBlocksLocal;
        TDynamicCounters::TCounterPtr Endpoints;

        void Register(TDynamicCounters& counters)
        {
            ReadBlocksLocal.Register(
                *counters.GetSubgroup("request", "ReadBlocksLocal"));
            WriteBlocksLocal.Register(
                *counters.GetSubgroup("request", "WriteBlocksLocal"));
            Endpoints = counters.GetCounter("Endpoints", /*derivative=*/false);
        }
    };

    TIntegrityCounters IntegrityModeCounters;
    TIntegrityCounters NormalModeCounters;
    // Data integrity mode doesn't affect normal read and write requests,
    TRequestCounters ReadBlocks;
    TRequestCounters WriteBlocks;

    void Register(TDynamicCounters& counters)
    {
        IntegrityModeCounters.Register(
            *counters.GetSubgroup("DataCopying", "enabled"));
        NormalModeCounters.Register(
            *counters.GetSubgroup("DataCopying", "disabled"));
        ReadBlocks.Register(*counters.GetSubgroup("request", "ReadBlocks"));
        WriteBlocks.Register(*counters.GetSubgroup("request", "WriteBlocks"));
    }

    void ReadBlocksCountIncrease()
    {
        ReadBlocks.Count->Inc();
    }

    void ReadBlocksLocalCountIncrease(bool integrityMode)
    {
        if (integrityMode) {
            IntegrityModeCounters.ReadBlocksLocal.Count->Inc();
        } else {
            NormalModeCounters.ReadBlocksLocal.Count->Inc();
        }
    }

    void WriteBlocksCountIncrease()
    {
        WriteBlocks.Count->Inc();
    }

    void WriteBlocksLocalCountIncrease(bool integrityMode)
    {
        if (integrityMode) {
            IntegrityModeCounters.WriteBlocksLocal.Count->Inc();
        } else {
            NormalModeCounters.WriteBlocksLocal.Count->Inc();
        }
    }

    void ReadBlocksMismatchIncrease()
    {
        ReadBlocks.Mismatches->Inc();
    }

    void ReadBlocksLocalMismatchIncrease(bool integrityMode)
    {
        if (integrityMode) {
            IntegrityModeCounters.ReadBlocksLocal.Mismatches->Inc();
        } else {
            NormalModeCounters.ReadBlocksLocal.Mismatches->Inc();
        }
    }

    void WriteBlocksMismatchIncrease()
    {
        WriteBlocks.Mismatches->Inc();
    }

    void WriteBlocksLocalMismatchIncrease(bool integrityMode)
    {
        if (integrityMode) {
            IntegrityModeCounters.WriteBlocksLocal.Mismatches->Inc();
        } else {
            NormalModeCounters.WriteBlocksLocal.Mismatches->Inc();
        }
    }

    void EndpointsIncrease(bool integrityMode)
    {
        if (integrityMode) {
            IntegrityModeCounters.Endpoints->Inc();
        } else {
            NormalModeCounters.Endpoints->Inc();
        }
    }

    void EndpointsDecrease(bool integrityMode)
    {
        if (integrityMode) {
            Y_DEBUG_ABORT_UNLESS(IntegrityModeCounters.Endpoints->Val() > 0);
            IntegrityModeCounters.Endpoints->Dec();
        } else {
            Y_DEBUG_ABORT_UNLESS(NormalModeCounters.Endpoints->Val() > 0);
            NormalModeCounters.Endpoints->Dec();
        }
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
    std::atomic_flag IntegrityModeEnabled;

    TDynamicCountersPtr Counters;
    TStatCounters StatCounters;

public:
    TDataIntegrityClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        TString diskId,
        ui32 blockSize,
        bool intergityModeEnabled);

    ~TDataIntegrityClient() override;

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
            response =                                                         \
                Client->name(std::move(callContext), std::move(request));      \
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

    void EnableCopyingForVolume();
};

////////////////////////////////////////////////////////////////////////////////

TDataIntegrityClient::TDataIntegrityClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        TString diskId,
        ui32 blockSize,
        bool intergityModeEnabled)
    : Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    , Client(std::move(client))
    , DiskId(std::move(diskId))
    , BlockSize(blockSize)
    , IntegrityModeEnabled(intergityModeEnabled)
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", "service")
                   ->GetSubgroup("subcomponent", "data_integrity");
    StatCounters.Register(*Counters);
    StatCounters.EndpointsIncrease(IntegrityModeEnabled.test());
}

TDataIntegrityClient::~TDataIntegrityClient()
{
    // The destruction of this object should happen after drain. So we should
    // not worry about changing counters non-atomically.
    StatCounters.EndpointsDecrease(IntegrityModeEnabled.test());
}

bool TDataIntegrityClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    TFuture<NProto::TReadBlocksResponse>& response)
{
    StatCounters.ReadBlocksCountIncrease();

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
                self->StatCounters.ReadBlocksMismatchIncrease();

                // Calling EnableCopyingForVolume() is pointless here, as the
                // client could not interfere with the data.

                ui32 flags = 0;
                SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
                SetProtoFlag(flags, NProto::EF_INSTANT_RETRIABLE);
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
    const bool integrityMode = IntegrityModeEnabled.test();
    StatCounters.ReadBlocksLocalCountIncrease(integrityMode);

    auto sgList = request->Sglist;
    const ui64 requestByteCount =
        static_cast<const ui64>(request->GetBlocksCount()) * request->BlockSize;

    auto responseHandler =
        [guardedSgList = std::move(sgList),
         weakPtr = weak_from_this(),
         requestByteCount,
         integrityMode]<typename TResponse>(
            TFuture<TResponse> result) -> NProto::TReadBlocksLocalResponse
    {
        NProto::TReadBlocksLocalResponse response{result.ExtractValue()};
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
        NProto::TChecksum currentChecksum;
        if (integrityMode) {
            const size_t bytesCopied = CopyToSgList(
                response.GetBlocks(),
                self->BlockSize,
                sgList,
                self->BlockSize);
            Y_DEBUG_ABORT_UNLESS(bytesCopied == requestByteCount);

            currentChecksum =
                CalculateChecksum(response.GetBlocks(), self->BlockSize);
        } else {
            currentChecksum = CalculateChecksum(sgList);
        }

        const auto& checksum = response.GetChecksum();
        if (!MessageDifferencer::Equals(checksum, currentChecksum)) {
            self->StatCounters.ReadBlocksLocalMismatchIncrease(integrityMode);
            self->EnableCopyingForVolume();

            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
            SetProtoFlag(flags, NProto::EF_INSTANT_RETRIABLE);
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
    };

    if (integrityMode) {
        response =
            Client->ReadBlocks(std::move(callContext), std::move(request))
                .Apply(responseHandler);
    } else {
        response =
            Client->ReadBlocksLocal(std::move(callContext), std::move(request))
                .Apply(responseHandler);
    }

    return true;
}

bool TDataIntegrityClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    TFuture<NProto::TWriteBlocksResponse>& response)
{
    StatCounters.WriteBlocksCountIncrease();

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
                        // Calling EnableCopyingForVolume() is pointless here,
                        // as the client could not interfere with the data.

                        self->StatCounters.WriteBlocksMismatchIncrease();
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
    const bool integrityMode = IntegrityModeEnabled.test();
    StatCounters.WriteBlocksLocalCountIncrease(integrityMode);

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        response = MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse{MakeError(
                E_CANCELLED,
                "failed to acquire sglist in DataIntegrityClient")});
        return true;
    }

    const auto& sgList = guard.Get();
    TVector<NProto::TChecksum> checksums;
    if (integrityMode) {
        NProto::TIOVector blocks;
        SgListCopy(
            sgList,
            ResizeIOVector(blocks, request->BlocksCount, request->BlockSize));
        *request->MutableBlocks() = std::move(blocks);

        checksums = CalculateChecksumsForWriteRequest(
            request->GetBlocks().GetBuffers(),
            request->GetStartIndex(),
            request->BlockSize);
    } else {
        checksums = CalculateChecksumsForWriteRequest(
            sgList,
            request->GetStartIndex(),
            BlockSize);
    }

    for (auto& checksum: checksums) {
        *request->AddChecksums() = std::move(checksum);
    }

    auto responseHandler = [weakPtr = weak_from_this(), integrityMode](
                               TFuture<NProto::TWriteBlocksResponse> result)
    {
        auto response = result.ExtractValue();
        auto self = weakPtr.lock();
        if (!self) {
            return response;
        }

        auto& error = *response.MutableError();
        if (HasError(error) &&
            HasProtoFlag(error.GetFlags(), NProto::EF_CHECKSUM_MISMATCH))
        {
            self->StatCounters.WriteBlocksLocalMismatchIncrease(integrityMode);
            self->EnableCopyingForVolume();
            SetErrorProtoFlag(error, NProto::EF_INSTANT_RETRIABLE);
        }

        return response;
    };

    if (integrityMode) {
        response =
            Client->WriteBlocks(std::move(callContext), std::move(request))
                .Apply(responseHandler);
    } else {
        response =
            Client->WriteBlocksLocal(std::move(callContext), std::move(request))
                .Apply(responseHandler);
    }

    return true;
}

void TDataIntegrityClient::EnableCopyingForVolume()
{
    if (IntegrityModeEnabled.test_and_set()) {
        return;
    }
    StatCounters.EndpointsDecrease(/*integrityMode=*/false);
    StatCounters.EndpointsIncrease(/*integrityMode=*/true);

    auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());
    auto request = std::make_shared<NProto::TExecuteActionRequest>();
    request->SetAction("modifytags");

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
    const bool intergityModeEnabled =
        volume.GetTags().contains(DataIntegrityViolationDetectedTagName) ||
        volume.GetTags().contains(IntermediateWriteBufferTagName);
    return std::make_unique<TDataIntegrityClient>(
        std::move(logging),
        std::move(monitoring),
        std::move(client),
        volume.GetDiskId(),
        volume.GetBlockSize(),
        intergityModeEnabled);
}

}   // namespace NCloud::NBlockStore::NClient
