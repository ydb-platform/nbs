#include "validation.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/monlib/service/service.h>

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

constexpr ui64 InvalidBlockIndex = Max();

using TBlockMap = google::dense_hash_map<ui64, ui64>;
using TRequestsInFlight = TList<TOperationRange>;

struct TVolume
{
    NProto::TVolume Info;
    TString SessionId;
    TBlockMap Blocks;
    TRequestsInFlight Requests;
    TMutex Lock;
    TAtomic ZeroBlockDigest = InvalidDigest;

    TVolume(const NProto::TVolume& info, const TString& sessionId)
        : Info(info)
        , SessionId(sessionId)
    {
        Blocks.set_empty_key(InvalidBlockIndex);
    }

    TString GetDiskId() const
    {
        return Info.GetDiskId();
    }
};

using TVolumePtr = std::shared_ptr<TVolume>;
using TVolumeMap = THashMap<TString, TVolumePtr>;

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

class TValidationClient final
    : public IBlockStoreValidationClient
{
    class TMonPage;

private:
    mutable TLog Log;

    const IBlockStorePtr Client;
    const IBlockDigestCalculatorPtr DigestCalculator;
    const IValidationCallbackPtr Callback;
    const TString LoggingTag;
    const TBlockRange64 ValidationRange;

    TDynamicCountersPtr Counters;
    TRequestCounters RequestCounters;

    TVolumeMap Volumes;
    TMutex Lock;

public:
    TValidationClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        IBlockDigestCalculatorPtr digestCalculator,
        IValidationCallbackPtr callback,
        TString loggingTag,
        const TBlockRange64& validationRange);

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

    void InitializeBlockChecksums(const TString& volumeName) override;
    void SetBlockChecksums(
        const TString& volumeName,
        const TVector<std::pair<ui64, ui64>>& checksums) override;
    void EnsureZeroBlockDigestInitialized(TVolume& volume);

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
    void OutputHtml(IOutputStream& out, const IMonHttpRequest& request);

    void ReportError(const TString& message) const;

    void ReportIncompleteRead(
        const TVolume& volume,
        ui32 requestedCount,
        ui32 reportedCount) const;

    void ReportInconsistentRead(
        const TVolume& volume,
        const TOperationRange& range,
        ui64 blockIndex,
        ui64 prevHash,
        ui64 newHash) const;

    void ReportRace(
        const TVolume& volume,
        const TOperationRange& newRange,
        const TOperationRange& oldRange) const;

    TVolumePtr MountVolume(
        const NProto::TVolume& info,
        const TString& sessionId);
    void UnmountVolume(const TString& diskId);
    TVolumePtr GetVolume(const TString& diskId) const;
    TMaybe<NProto::TVolume> GetVolumeInfo(const TString& diskId) const;
    TString GetSessionId(const TString& diskId) const;

    TMaybe<TOperationRange> CreateRange(
        ui64 startIndex,
        ui32 blocksCount,
        EOperation op);

    void EnterRange(TVolume& volume, const TOperationRange& range);
    void LeaveRange(TVolume& volume, const TOperationRange& range);

    void OnError(TVolume& volume, const TOperationRange& range);

    TVector<ui64> PrepareRead(
        TVolume& volume,
        const TOperationRange& range);

    void CompleteRead(
        TVolume& volume,
        const TOperationRange& range,
        const TVector<ui64>& blocksWritten,
        const TVector<ui64>& blocksRead);

    void PrepareWrite(
        TVolume& volume,
        const TOperationRange& range);

    void CompleteWrite(
        TVolume& volume,
        const TOperationRange& range,
        const TVector<ui64>& blocks);

    void PrepareZero(
        TVolume& volume,
        const TOperationRange& range);

    void CompleteZero(
        TVolume& volume,
        const TOperationRange& range);

    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request,
        TFuture<NProto::TMountVolumeResponse>& response);

    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request,
        TFuture<NProto::TUnmountVolumeResponse>& response);

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

    bool HandleRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        TFuture<NProto::TZeroBlocksResponse>& response);

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

class TValidationClient::TMonPage final
    : public THtmlMonPage
{
private:
    TValidationClient& Client;

public:
    TMonPage(TValidationClient& client)
        : THtmlMonPage("validation", "Validation", true)
        , Client(client)
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        Client.OutputHtml(request.Output(), request);
    }
};

////////////////////////////////////////////////////////////////////////////////

TValidationClient::TValidationClient(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr client,
        IBlockDigestCalculatorPtr digestCalculator,
        IValidationCallbackPtr callback,
        TString loggingTag,
        const TBlockRange64& validationRange)
    : Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
    , Client(std::move(client))
    , DigestCalculator(std::move(digestCalculator))
    , Callback(std::move(callback))
    , LoggingTag(std::move(loggingTag))
    , ValidationRange(validationRange)
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", "validation");
    RequestCounters.Register(*Counters);

    auto rootPage = monitoring->RegisterIndexPage("blockstore", "BlockStore");
    static_cast<TIndexMonPage&>(*rootPage).Register(new TMonPage(*this));
}

void TValidationClient::InitializeBlockChecksums(
    const TString& volumeName)
{
    TVolumePtr volume = GetVolume(volumeName);

    if (!volume) {
        ReportError(
            TStringBuilder()
                << "InitializeBlockChecksums attempt of unknown volume: "
                << volumeName);
        return;
    }

    EnsureZeroBlockDigestInitialized(*volume);

    with_lock (volume->Lock) {
        for (auto b: xrange(ValidationRange)) {
            volume->Blocks[b] = AtomicGet(volume->ZeroBlockDigest);
        }
    }
}

void TValidationClient::SetBlockChecksums(
    const TString& volumeName,
    const TVector<std::pair<ui64, ui64>>& checksums)
{
    TVolumePtr volume = GetVolume(volumeName);

    if (!volume) {
        ReportError(
            TStringBuilder()
                << "SetBlockChecksums attempt of unknown volume: "
                << volumeName);
        return;
    }

    with_lock (volume->Lock) {
        for (const auto& x: checksums) {
            if (ValidationRange.Contains(x.first)) {
                volume->Blocks[x.first] = x.second;
            }
        }
    }
}

void TValidationClient::EnsureZeroBlockDigestInitialized(TVolume& volume)
{
    if (AtomicGet(volume.ZeroBlockDigest) == InvalidDigest) {
        with_lock (volume.Lock) {
            if (AtomicGet(volume.ZeroBlockDigest) == InvalidDigest) {
                TVector<char> buffer(volume.Info.GetBlockSize());
                AtomicSet(volume.ZeroBlockDigest, DigestCalculator->Calculate(
                    InvalidBlockIndex,
                    {
                        buffer.data(),
                        buffer.size()
                    }
                ));
                Y_ABORT_UNLESS(AtomicGet(volume.ZeroBlockDigest) != InvalidDigest);
            }
        }
    }
}

void TValidationClient::OutputHtml(
    IOutputStream& out,
    const IMonHttpRequest& request)
{
    Y_UNUSED(request);

    HTML(out) {
        TAG(TH3) { out << "Counters"; }
        Counters->OutputHtml(out);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TValidationClient::ReportError(const TString& message) const
{
    STORAGE_ERROR(LoggingTag << "Validation error: " << message);

    if (Callback) {
        Callback->ReportError(message);
    }
}

void TValidationClient::ReportIncompleteRead(
    const TVolume& volume,
    ui32 requestedCount,
    ui32 reportedCount) const
{
    ReportError(TStringBuilder()
        << "[" << volume.GetDiskId() << "] "
        << "read " << requestedCount << " blocks but "
        << reportedCount << " returned");
}

void TValidationClient::ReportInconsistentRead(
    const TVolume& volume,
    const TOperationRange& range,
    ui64 blockIndex,
    ui64 prevHash,
    ui64 newHash) const
{
    RequestCounters.InconsistentRead->Inc();

    ReportError(TStringBuilder()
        << "[" << volume.GetDiskId() << "] "
        << "read inconsistency for range " << range << " in block " << blockIndex
        << " written " << prevHash << " read " << newHash);
}

void TValidationClient::ReportRace(
    const TVolume& volume,
    const TOperationRange& newRange,
    const TOperationRange& oldRange) const
{
    if (newRange.Op != oldRange.Op) {
        RequestCounters.ReadWriteRace->Inc();
    } else {
        RequestCounters.WriteWriteRace->Inc();
    }

    ReportError(TStringBuilder()
        << "[" << volume.GetDiskId() << "] "
        << newRange << " overlaps with " << oldRange);
}

////////////////////////////////////////////////////////////////////////////////

TVolumePtr TValidationClient::MountVolume(
    const NProto::TVolume& info,
    const TString& sessionId)
{
    with_lock (Lock) {
        TVolumeMap::iterator it;
        bool inserted;

        std::tie(it, inserted) = Volumes.emplace(
            info.GetDiskId(),
            std::make_shared<TVolume>(info, sessionId));

        if (inserted) {
            RequestCounters.MountedVolumes->Inc();
        }

        return it->second;
    }
}

void TValidationClient::UnmountVolume(const TString& diskId)
{
    with_lock (Lock) {
        auto it = Volumes.find(diskId);
        if (it != Volumes.end()) {
            TVolumePtr volume = it->second;
            with_lock (volume->Lock) {
                RequestCounters.BlocksCount->Sub(volume->Blocks.size());
            }
            Volumes.erase(it);
            RequestCounters.MountedVolumes->Dec();
        } else {
            ReportError(TStringBuilder()
                << "unknown volume: " << diskId);
        }
    }
}

TVolumePtr TValidationClient::GetVolume(const TString& diskId) const
{
    with_lock (Lock) {
        auto it = Volumes.find(diskId);
        if (it == Volumes.end()) {
            ReportError(TStringBuilder()
                << "unknown volume: " << diskId);
            return {};
        }

        return it->second;
    }
}

TMaybe<NProto::TVolume> TValidationClient::GetVolumeInfo(
    const TString& diskId) const
{
    with_lock (Lock) {
        auto it = Volumes.find(diskId);
        if (it == Volumes.end()) {
            ReportError(TStringBuilder()
                << "unknown volume: " << diskId);
            return {};
        }

        return it->second->Info;
    }
}

TString TValidationClient::GetSessionId(const TString& diskId) const
{
    with_lock (Lock) {
        auto it = Volumes.find(diskId);
        if (it == Volumes.end()) {
            ReportError(TStringBuilder()
                << "unknown volume: " << diskId);
            return {};
        }

        return it->second->SessionId;
    }
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<TOperationRange> TValidationClient::CreateRange(
    ui64 startIndex,
    ui32 blocksCount,
    EOperation op)
{
    const auto blockRange = TBlockRange64::WithLength(startIndex, blocksCount);

    if (!blockRange.Overlaps(ValidationRange)) {
        // ignore range completely
        return Nothing();
    }

    const auto intersection = blockRange.Intersect(ValidationRange);

    return TOperationRange {
        intersection.Start,
        intersection.End,
        op
    };
}

void TValidationClient::EnterRange(
    TVolume& volume,
    const TOperationRange& range)
{
    for (const auto& r: volume.Requests) {
        if (range.Overlaps(r)) {
            if (range.Op == EOperation::Read && r.Op == EOperation::Read) {
                // read/read race is ok
                continue;
            }
            ReportRace(volume, range, r);
        }
    }

    volume.Requests.push_back(range);
}

void TValidationClient::LeaveRange(
    TVolume& volume,
    const TOperationRange& range)
{
    auto it = std::find(volume.Requests.begin(), volume.Requests.end(), range);
    Y_ABORT_UNLESS(it != volume.Requests.end());

    volume.Requests.erase(it);
}

void TValidationClient::OnError(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        LeaveRange(volume, range);
    }
}

TVector<ui64> TValidationClient::PrepareRead(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        EnterRange(volume, range);

        TVector<ui64> result(Reserve(range.Size()));
        for (ui64 i = 0; i < range.Size(); ++i) {
            auto it = volume.Blocks.find(range.Begin + i);
            if (it != volume.Blocks.end()) {
                result.push_back(it->second);
            } else {
                result.push_back(InvalidDigest);
            }
        }

        return result;
    }
}

void TValidationClient::CompleteRead(
    TVolume& volume,
    const TOperationRange& range,
    const TVector<ui64>& blocksWritten,
    const TVector<ui64>& blocksRead)
{
    with_lock (volume.Lock) {
        if (blocksWritten.size() != blocksRead.size()) {
            ReportIncompleteRead(
                volume,
                blocksWritten.size(),
                blocksRead.size());
        }

        for (ui32 i = 0; i < Min(blocksWritten.size(), blocksRead.size()); ++i) {
            if (blocksWritten[i] == InvalidDigest) {
                // we did not see this block before -> initialize its digest
                volume.Blocks[range.Begin + i] = blocksRead[i];
                continue;
            }

            if (blocksWritten[i] != blocksRead[i]) {
                ReportInconsistentRead(
                    volume,
                    range,
                    range.Begin + i,
                    blocksWritten[i],
                    blocksRead[i]);
            }
        }

        LeaveRange(volume, range);
    }
}

void TValidationClient::PrepareWrite(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        EnterRange(volume, range);
    }
}

void TValidationClient::CompleteWrite(
    TVolume& volume,
    const TOperationRange& range,
    const TVector<ui64>& blocks)
{
    with_lock (volume.Lock) {
        auto oldSize = volume.Blocks.size();

        for (ui32 i = 0; i < blocks.size(); ++i) {
            volume.Blocks[range.Begin + i] = blocks[i];
        }

        RequestCounters.BlocksCount->Add(volume.Blocks.size() - oldSize);
        LeaveRange(volume, range);
    }
}

void TValidationClient::PrepareZero(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        EnterRange(volume, range);
    }
}

void TValidationClient::CompleteZero(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        auto oldSize = volume.Blocks.size();

        for (ui32 i = 0; i < range.Size(); ++i) {
            volume.Blocks[range.Begin + i] = AtomicGet(volume.ZeroBlockDigest);
        }

        RequestCounters.BlocksCount->Add(volume.Blocks.size() - oldSize);
        LeaveRange(volume, range);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TValidationClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TMountVolumeRequest> request,
    TFuture<NProto::TMountVolumeResponse>& response)
{
    RequestCounters.MountRequests->Inc();

    response = Client->MountVolume(
        std::move(callContext), std::move(request)
    ).Subscribe([this] (const auto& future) {
        if (!IsRequestFailed(future)) {
            const auto& response = future.GetValue();
            MountVolume(
                response.GetVolume(),
                response.GetSessionId());
        }
    });
    return true;
}

bool TValidationClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TUnmountVolumeRequest> request,
    TFuture<NProto::TUnmountVolumeResponse>& response)
{
    RequestCounters.UnmountRequests->Inc();

    auto diskId = request->GetDiskId();
    response = Client->UnmountVolume(
        std::move(callContext), std::move(request)
    ).Subscribe([=, this] (const auto& future) {
        if (!IsRequestFailed(future)) {
            UnmountVolume(diskId);
        }
    });
    return true;
}

bool TValidationClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    TFuture<NProto::TReadBlocksResponse>& response)
{
    RequestCounters.ReadRequests->Inc();

    auto volume = GetVolume(request->GetDiskId());
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    EnsureZeroBlockDigestInitialized(*volume);

    auto range = CreateRange(
        request->GetStartIndex(),
        request->GetBlocksCount(),
        EOperation::Read);

    if (!range) {
        // ignore invalid range
        return false;
    }

    auto blocks = PrepareRead(*volume, *range);
    Y_ABORT_UNLESS(blocks.size() == range->Size());

    const auto zeroBlockDigest = AtomicGet(volume->ZeroBlockDigest);

    auto f = Client->ReadBlocks(
        std::move(callContext), std::move(request)
    );

    response = f.Subscribe([=, this, blocks_ = std::move(blocks)] (const auto& future) {
        if (!IsRequestFailed(future)) {
            const auto& response = future.GetValue();

            auto sgListOrError = GetSgList(response, volume->Info.GetBlockSize());
            if (HasError(sgListOrError)) {
                OnError(*volume, *range);
                return;
            }

            auto blocksRead = CalculateBlocksDigest(
                sgListOrError.GetResult(),
                *DigestCalculator,
                volume->Info.GetBlockSize(),
                range->Begin,
                range->Size(),
                zeroBlockDigest
            );

            CompleteRead(*volume, *range, blocks_, blocksRead);
        } else {
            OnError(*volume, *range);
        }
        return;
    });

    return true;
}

bool TValidationClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    TFuture<NProto::TReadBlocksLocalResponse>& response)
{
    RequestCounters.ReadRequests->Inc();

    auto volume = GetVolume(request->GetDiskId());
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    EnsureZeroBlockDigestInitialized(*volume);

    auto range = CreateRange(
        request->GetStartIndex(),
        request->GetBlocksCount(),
        EOperation::Read);

    if (!range) {
        // ignore invalid range
        return false;
    }

    auto blocks = PrepareRead(*volume, *range);
    Y_ABORT_UNLESS(blocks.size() == range->Size());

    const auto zeroBlockDigest = AtomicGet(volume->ZeroBlockDigest);
    auto sgList = request->Sglist;

    auto f = Client->ReadBlocksLocal(std::move(callContext), request);

    response = f.Subscribe([=, this, blocks = std::move(blocks)] (const auto& future) {
        if (!IsRequestFailed(future)) {
            auto guard = sgList.Acquire();

            if (guard) {
                auto blocksRead = CalculateBlocksDigest(
                    guard.Get(),
                    *DigestCalculator,
                    volume->Info.GetBlockSize(),
                    range->Begin,
                    range->Size(),
                    zeroBlockDigest
                );

                CompleteRead(*volume, *range, blocks, blocksRead);
                return;
            }
        }

        OnError(*volume, *range);
    });

    return true;
}

bool TValidationClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    TFuture<NProto::TWriteBlocksResponse>& response)
{
    RequestCounters.WriteRequests->Inc();

    auto volume = GetVolume(request->GetDiskId());
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    EnsureZeroBlockDigestInitialized(*volume);

    auto sgListOrError = SgListNormalize(
        GetSgList(*request),
        volume->Info.GetBlockSize());

    if (HasError(sgListOrError)) {
        // ignore invalid buffer
        return false;
    }

    const auto& sglist = sgListOrError.GetResult();

    auto range = CreateRange(
        request->GetStartIndex(),
        sglist.size(),
        EOperation::Write);

    if (!range) {
        // ignore invalid range
        return false;
    }

    auto blocks = CalculateBlocksDigest(
        sglist,
        *DigestCalculator,
        volume->Info.GetBlockSize(),
        range->Begin,
        range->Size(),
        AtomicGet(volume->ZeroBlockDigest)
    );

    PrepareWrite(*volume, *range);

    response = Client->WriteBlocks(
        std::move(callContext), std::move(request)
    ).Subscribe([=, this, blocks_ = std::move(blocks)] (const auto& future) {
        if (!IsRequestFailed(future)) {
            CompleteWrite(*volume, *range, blocks_);
        } else {
            OnError(*volume, *range);
        }
    });

    return true;
}

bool TValidationClient::HandleRequest(
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

bool TValidationClient::HandleRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    TFuture<NProto::TZeroBlocksResponse>& response)
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
        request->GetBlocksCount(),
        EOperation::Zero);

    if (!range) {
        // ignore invalid range
        return false;
    }

    PrepareZero(*volume, *range);

    response = Client->ZeroBlocks(
        std::move(callContext), std::move(request)
    ).Subscribe([=, this] (const auto& future) {
        if (!IsRequestFailed(future)) {
            CompleteZero(*volume, *range);
        } else {
            OnError(*volume, *range);
        }
    });
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStoreValidationClientPtr CreateValidationClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr client,
    IBlockDigestCalculatorPtr digestCalculator,
    IValidationCallbackPtr callback,
    TString loggingTag,
    const TBlockRange64& validationRange)
{
    return std::make_unique<TValidationClient>(
        std::move(logging),
        std::move(monitoring),
        std::move(client),
        std::move(digestCalculator),
        std::move(callback),
        std::move(loggingTag),
        validationRange);
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
