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

#include <util/generic/scope.h>
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

TStorageBuffer AllocateStorageBuffer(IBlockStore& client, ui32 bytesCount)
{
    auto buffer = client.AllocateBuffer(bytesCount);

    if (!buffer) {
        buffer = std::shared_ptr<char>(
            new char[bytesCount],
            std::default_delete<char[]>());
    }
    return buffer;
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

    struct TValidationCounters
    {
        TRequestCounters ReadBlocksLocal;
        TRequestCounters WriteBlocksLocal;
        TDynamicCounters::TCounterPtr Clients;

        void Register(TDynamicCounters& counters)
        {
            ReadBlocksLocal.Register(
                *counters.GetSubgroup("request", "ReadBlocksLocal"));
            WriteBlocksLocal.Register(
                *counters.GetSubgroup("request", "WriteBlocksLocal"));
            Clients = counters.GetCounter("Clients", /*derivative=*/false);
        }
    };

    TValidationCounters CopiedValidationModeCounters;
    TValidationCounters DirectValidationModeCounters;
    // Validation mode doesn't affect non-local requests.
    TRequestCounters ReadBlocks;
    TRequestCounters WriteBlocks;

    void Register(TDynamicCounters& counters)
    {
        CopiedValidationModeCounters.Register(
            *counters.GetSubgroup("validation_mode", "copied"));
        DirectValidationModeCounters.Register(
            *counters.GetSubgroup("validation_mode", "direct"));
        ReadBlocks.Register(*counters.GetSubgroup("request", "ReadBlocks"));
        WriteBlocks.Register(*counters.GetSubgroup("request", "WriteBlocks"));
    }

    void ReadBlocksCountIncrease()
    {
        ReadBlocks.Count->Inc();
    }

    void ReadBlocksLocalCountIncrease(bool shouldCopy)
    {
        if (shouldCopy) {
            CopiedValidationModeCounters.ReadBlocksLocal.Count->Inc();
        } else {
            DirectValidationModeCounters.ReadBlocksLocal.Count->Inc();
        }
    }

    void WriteBlocksCountIncrease()
    {
        WriteBlocks.Count->Inc();
    }

    void WriteBlocksLocalCountIncrease(bool shouldCopy)
    {
        if (shouldCopy) {
            CopiedValidationModeCounters.WriteBlocksLocal.Count->Inc();
        } else {
            DirectValidationModeCounters.WriteBlocksLocal.Count->Inc();
        }
    }

    void ReadBlocksMismatchIncrease()
    {
        ReadBlocks.Mismatches->Inc();
    }

    void ReadBlocksLocalMismatchIncrease(bool shouldCopy)
    {
        if (shouldCopy) {
            CopiedValidationModeCounters.ReadBlocksLocal.Mismatches->Inc();
        } else {
            DirectValidationModeCounters.ReadBlocksLocal.Mismatches->Inc();
        }
    }

    void WriteBlocksMismatchIncrease()
    {
        WriteBlocks.Mismatches->Inc();
    }

    void WriteBlocksLocalMismatchIncrease(bool shouldCopy)
    {
        if (shouldCopy) {
            CopiedValidationModeCounters.WriteBlocksLocal.Mismatches->Inc();
        } else {
            DirectValidationModeCounters.WriteBlocksLocal.Mismatches->Inc();
        }
    }

    void ClientsIncrease(bool shouldCopy)
    {
        if (shouldCopy) {
            CopiedValidationModeCounters.Clients->Inc();
        } else {
            DirectValidationModeCounters.Clients->Inc();
        }
    }

    void ClientsDecrease(bool shouldCopy)
    {
        if (shouldCopy) {
            Y_DEBUG_ABORT_UNLESS(
                CopiedValidationModeCounters.Clients->Val() > 0);
            CopiedValidationModeCounters.Clients->Dec();
        } else {
            Y_DEBUG_ABORT_UNLESS(
                DirectValidationModeCounters.Clients->Val() > 0);
            DirectValidationModeCounters.Clients->Dec();
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
    std::atomic_flag CopiedDataValidationEnabled;

    TDynamicCountersPtr Counters;
    TStatCounters StatCounters;

public:
    TDataIntegrityClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        TString diskId,
        ui32 blockSize,
        bool copiedDataValidationEnabled);

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
        bool copiedDataValidationEnabled)
    : Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    , Client(std::move(client))
    , DiskId(std::move(diskId))
    , BlockSize(blockSize)
    , CopiedDataValidationEnabled(copiedDataValidationEnabled)
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", "service")
                   ->GetSubgroup("subcomponent", "data_integrity");
    StatCounters.Register(*Counters);
    StatCounters.ClientsIncrease(CopiedDataValidationEnabled.test());
}

TDataIntegrityClient::~TDataIntegrityClient()
{
    // The destruction of this object should happen after drain. So we should
    // not worry about changing counters non-atomically.
    StatCounters.ClientsDecrease(CopiedDataValidationEnabled.test());
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
                        << currentChecksum.ShortUtf8DebugString().Quote()
                        << "; Incoming checksum: "
                        << checksum.ShortUtf8DebugString().Quote(),
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
    const bool shouldCopy = CopiedDataValidationEnabled.test();
    StatCounters.ReadBlocksLocalCountIncrease(shouldCopy);

    const ui32 requestByteCount =
        request->GetBlocksCount() * request->GetBlockSize();
    TStorageBuffer copyBuffer;
    TGuardedSgList copyGuardedSgList;
    auto requestCopy =
        std::make_shared<NProto::TReadBlocksLocalRequest>(*request);
    if (shouldCopy) {
        copyBuffer = AllocateStorageBuffer(*Client, requestByteCount);
        auto sgListOrError =
            SgListNormalize({copyBuffer.get(), requestByteCount}, BlockSize);
        if (HasError(sgListOrError)) {
            response = MakeFuture<NProto::TReadBlocksLocalResponse>(
                TErrorResponse(sgListOrError.GetError()));
            return true;
        }
        copyGuardedSgList = TGuardedSgList(sgListOrError.ExtractResult());
        requestCopy->Sglist = copyGuardedSgList.CreateDepender();
    }

    auto responseHandler =
        [shouldCopy,
         requestByteCount,
         weakPtr = weak_from_this(),
         request = std::move(request),
         copyGuardedSgList = std::move(copyGuardedSgList),
         copyBuffer = std::move(copyBuffer)](
            TFuture<NProto::TReadBlocksLocalResponse> result) mutable
        -> NProto::TReadBlocksLocalResponse
    {
        Y_DEFER
        {
            if (!copyGuardedSgList.Empty()) {
                copyGuardedSgList.Close();
                copyBuffer.reset();
            }
        };

        NProto::TReadBlocksLocalResponse response{result.ExtractValue()};
        if (HasError(response)) {
            return response;
        }

        auto self = weakPtr.lock();
        if (!self) {
            return response;
        }

        auto guard = request->Sglist.Acquire();
        if (!guard) {
            return TErrorResponse{MakeError(
                E_CANCELLED,
                "failed to acquire sglist in DataIntegrityClient")};
        }

        const TSgList* sgList = nullptr;
        if (shouldCopy) {
            // Should be safe, since we are the only one who can close the
            // sglist.
            sgList = &copyGuardedSgList.Acquire().Get();
            // Once the sglist is closed, no one should change the contents of
            // the buffer. Now it is safe to copy and calculate the checksum.
            copyGuardedSgList.Close();

            const size_t bytesCopied = SgListCopy(*sgList, guard.Get());
            Y_DEBUG_ABORT_UNLESS(requestByteCount == bytesCopied);
        } else {
            sgList = &guard.Get();
        }

        if (!response.HasChecksum()) {
            return response;
        }

        NProto::TChecksum currentChecksum = CalculateChecksum(*sgList);
        Y_DEBUG_ABORT_UNLESS(
            requestByteCount == currentChecksum.GetByteCount());

        const auto& checksum = response.GetChecksum();
        if (!MessageDifferencer::Equals(checksum, currentChecksum)) {
            self->StatCounters.ReadBlocksLocalMismatchIncrease(shouldCopy);
            self->EnableCopyingForVolume();

            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_CHECKSUM_MISMATCH);
            SetProtoFlag(flags, NProto::EF_INSTANT_RETRIABLE);
            return TErrorResponse{MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Data integrity violation. Current checksum: "
                    << currentChecksum.ShortUtf8DebugString().Quote()
                    << "; Incoming checksum: "
                    << checksum.ShortUtf8DebugString().Quote(),
                flags)};
        }

        return response;
    };

    response =
        Client->ReadBlocksLocal(std::move(callContext), std::move(requestCopy))
            .Apply(responseHandler);

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
        request->GetBlockSize());
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
    const bool shouldCopy = CopiedDataValidationEnabled.test();
    StatCounters.WriteBlocksLocalCountIncrease(shouldCopy);

    auto guard = request->Sglist.Acquire();
    if (!guard) {
        response = MakeFuture<NProto::TWriteBlocksLocalResponse>(
            TErrorResponse{MakeError(
                E_CANCELLED,
                "failed to acquire sglist in DataIntegrityClient")});
        return true;
    }

    const ui32 requestByteCount =
        request->BlocksCount * request->GetBlockSize();
    TStorageBuffer copyBuffer;
    TGuardedSgList copyGuardedSgList;
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> requestCopy;
    if (shouldCopy) {
        copyBuffer = AllocateStorageBuffer(*Client, requestByteCount);
        auto sgListOrError =
            SgListNormalize({copyBuffer.get(), requestByteCount}, BlockSize);
        if (HasError(sgListOrError)) {
            response = MakeFuture<NProto::TWriteBlocksLocalResponse>(
                TErrorResponse(sgListOrError.GetError()));
            return true;
        }
        TSgList copySgList = sgListOrError.ExtractResult();

        const size_t bytesCopied = SgListCopy(guard.Get(), copySgList);
        Y_DEBUG_ABORT_UNLESS(requestByteCount == bytesCopied);

        copyGuardedSgList = TGuardedSgList(std::move(copySgList));
        requestCopy =
            std::make_shared<NProto::TWriteBlocksLocalRequest>(*request);
        requestCopy->Sglist = copyGuardedSgList.CreateDepender();
    } else {
        requestCopy = std::move(request);
    }

    // Safe to Acquire. Either we own the sgList or the guard already exists.
    TVector<NProto::TChecksum> checksums = CalculateChecksumsForWriteRequest(
        requestCopy->Sglist.Acquire().Get(),
        requestCopy->GetStartIndex(),
        requestCopy->GetBlockSize());
    Y_DEBUG_ABORT_UNLESS(requestCopy->GetChecksums().empty());
    for (auto& checksum: checksums) {
        *requestCopy->AddChecksums() = std::move(checksum);
    }

    auto responseHandler =
        [shouldCopy,
         weakPtr = weak_from_this(),
         copyBuffer = std::move(copyBuffer),
         copyGuardedSgList = std::move(copyGuardedSgList)](
            TFuture<NProto::TWriteBlocksResponse> result) mutable
    {
        if (!copyGuardedSgList.Empty()) {
            copyGuardedSgList.Close();
            copyBuffer.reset();
        }

        auto response = result.ExtractValue();
        auto self = weakPtr.lock();
        if (!self) {
            return response;
        }

        auto& error = *response.MutableError();
        if (HasError(error) &&
            HasProtoFlag(error.GetFlags(), NProto::EF_CHECKSUM_MISMATCH))
        {
            self->StatCounters.WriteBlocksLocalMismatchIncrease(shouldCopy);
            self->EnableCopyingForVolume();
            SetErrorProtoFlag(error, NProto::EF_INSTANT_RETRIABLE);
        }

        return response;
    };

    response =
        Client->WriteBlocksLocal(std::move(callContext), std::move(requestCopy))
            .Apply(responseHandler);

    return true;
}

void TDataIntegrityClient::EnableCopyingForVolume()
{
    if (CopiedDataValidationEnabled.test_and_set()) {
        return;
    }
    STORAGE_WARN(
        "Enabling data integrity mode for disk %s",
        DiskId.Quote().c_str());

    StatCounters.ClientsDecrease(/*shouldCopy=*/false);
    StatCounters.ClientsIncrease(/*shouldCopy=*/true);

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
    const bool copiedDataValidationEnabled =
        volume.GetTags().contains(DataIntegrityViolationDetectedTagName) ||
        volume.GetTags().contains(IntermediateWriteBufferTagName);
    return std::make_unique<TDataIntegrityClient>(
        std::move(logging),
        std::move(monitoring),
        std::move(client),
        volume.GetDiskId(),
        volume.GetBlockSize(),
        copiedDataValidationEnabled);
}

}   // namespace NCloud::NBlockStore::NClient
