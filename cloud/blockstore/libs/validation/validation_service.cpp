#include "validation.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <contrib/libs/sparsehash/src/sparsehash/dense_hash_map>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NServer {

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

TAtomicBase GenerateRequestId()
{
    static TAtomic id = 0;
    return AtomicIncrement(id);
}

struct TOperationRange
{
    TAtomicBase Id;
    ui64 Begin;
    ui64 End;
    EOperation Op;
    bool RaceDetected;

    TOperationRange(TAtomicBase id, ui64 begin, ui64 end, EOperation op)
        : Id(id)
        , Begin(begin)
        , End(end)
        , Op(op)
        , RaceDetected(false)
    {
    }

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
        return Id == other.Id;
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 InvalidBlockIndex = Max();

using TBlockMap = google::dense_hash_map<ui64, ui64>;
using TRequestsInFlight = TList<TOperationRange>;

struct TVolume
{
    TString DiskId;
    ui32 BlockSize;
    THashMap<TString, std::pair<TInstant, TDuration>> ClientIds;
    TBlockMap Blocks;
    TRequestsInFlight Requests;
    TMutex Lock;
    TAtomic ZeroBlockDigest;

    TVolume(TString diskId, ui32 blockSize, TAtomicBase zeroBlockDigest)
        : DiskId(std::move(diskId))
        , BlockSize(blockSize)
        , ZeroBlockDigest(zeroBlockDigest)
    {
        Blocks.set_empty_key(InvalidBlockIndex);
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

class TValidationService final
    : public IBlockStore
{
    class TMonPage;

private:
    static constexpr ui32 MaxBlockIndex = 25*1024*1024;   // 100GB in 4K blocks
    static_assert(MaxBlockIndex < InvalidBlockIndex, "");

    mutable TLog Log;

    const IBlockStorePtr Service;
    const IBlockDigestCalculatorPtr DigestCalculator;
    const TDuration InactiveClientsTimeout;
    const IValidationCallbackPtr Callback;

    TDynamicCountersPtr Counters;
    TRequestCounters RequestCounters;

    TVolumeMap Volumes;
    TMutex Lock;

public:
    TValidationService(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr service,
        IBlockDigestCalculatorPtr digestCalculator,
        TDuration inactiveClientsTimeout,
        IValidationCallbackPtr callback);

    void Start() override
    {
        Service->Start();
    }

    void Stop() override
    {
        Service->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        TFuture<NProto::T##name##Response> response;                           \
        if (!HandleRequest(ctx, request, response)) {                          \
            response = Service->name(std::move(ctx), std::move(request));      \
        }                                                                      \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    void OutputHtml(IOutputStream& out, const IMonHttpRequest& request);

    void ReportError(const TString& message) const;
    void ReportWarning(const TString& message) const;

    void ReportIncompleteRead(
        const TVolume& volume,
        ui32 requestedCount,
        ui32 reportedCount) const;

    void ReportInconsistentRead(
        const TVolume& volume,
        ui64 blockIndex,
        ui64 prevHash,
        ui64 newHash) const;

    void ReportRace(
        const TVolume& volume,
        const TOperationRange& newRange,
        const TOperationRange& oldRange) const;

    TVolumePtr MountVolume(
        const TString& diskId,
        const TString& clientId,
        ui32 blockSize,
        TDuration remountTimeout);

    void UnmountVolume(const TString& diskId, const TString& clientId);
    TVolumePtr GetVolume(
        const TString& diskId,
        const TString& clientId);

    static TMaybe<TOperationRange> CreateRange(
        ui64 startIndex,
        ui32 blocksCount,
        EOperation op);

    void EnterRange(TVolume& volume, TOperationRange range);
    void LeaveRange(TVolume& volume, TRequestsInFlight::iterator range);

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
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TMountVolumeRequest> request,
        TFuture<NProto::TMountVolumeResponse>& response);

    bool HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request,
        TFuture<NProto::TUnmountVolumeResponse>& response);

    bool HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TReadBlocksRequest> request,
        TFuture<NProto::TReadBlocksResponse>& response);

    bool HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
        TFuture<NProto::TReadBlocksLocalResponse>& response);

    bool HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        TFuture<NProto::TWriteBlocksResponse>& response);

    bool HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
        TFuture<NProto::TWriteBlocksLocalResponse>& response);

    bool HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TZeroBlocksRequest> request,
        TFuture<NProto::TZeroBlocksResponse>& response);

    template <typename TRequest, typename TResponse>
    bool HandleRequest(
        TCallContextPtr ctx,
        std::shared_ptr<TRequest> request,
        TFuture<TResponse>& response)
    {
        Y_UNUSED(ctx);
        Y_UNUSED(request);
        Y_UNUSED(response);
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TValidationService::TMonPage final
    : public THtmlMonPage
{
private:
    TValidationService& Service;

public:
    TMonPage(TValidationService& service)
        : THtmlMonPage("validation", "Validation", true)
        , Service(service)
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        Service.OutputHtml(request.Output(), request);
    }
};

////////////////////////////////////////////////////////////////////////////////

TValidationService::TValidationService(
        ILoggingServicePtr logging,
        IMonitoringServicePtr monitoring,
        IBlockStorePtr service,
        IBlockDigestCalculatorPtr digestCalculator,
        TDuration inactiveClientsTimeout,
        IValidationCallbackPtr callback)
    : Log(logging->CreateLog("BLOCKSTORE_SERVER"))
    , Service(std::move(service))
    , DigestCalculator(std::move(digestCalculator))
    , InactiveClientsTimeout(inactiveClientsTimeout)
    , Callback(std::move(callback))
{
    auto counters = monitoring->GetCounters();
    auto rootGroup = counters->GetSubgroup("counters", "blockstore");
    Counters = rootGroup->GetSubgroup("component", "validation");
    RequestCounters.Register(*Counters);

    auto rootPage = monitoring->RegisterIndexPage("blockstore", "BlockStore");
    static_cast<TIndexMonPage&>(*rootPage).Register(new TMonPage(*this));
}

void TValidationService::OutputHtml(
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

void TValidationService::ReportError(const TString& message) const
{
    STORAGE_ERROR("Validation error: " << message);

    if (Callback) {
        Callback->ReportError(message);
    }
}

void TValidationService::ReportWarning(const TString& message) const
{
    STORAGE_WARN("Validation warning: " << message);
}

void TValidationService::ReportIncompleteRead(
    const TVolume& volume,
    ui32 requestedCount,
    ui32 reportedCount) const
{
    ReportError(TStringBuilder()
        << "[" << volume.DiskId << "] "
        << "read " << requestedCount << " blocks but "
        << reportedCount << " returned");
}

void TValidationService::ReportInconsistentRead(
    const TVolume& volume,
    ui64 blockIndex,
    ui64 prevHash,
    ui64 newHash) const
{
    RequestCounters.InconsistentRead->Inc();

    ReportError(TStringBuilder()
        << "[" << volume.DiskId << "] "
        << "read inconsistency in block " << blockIndex
        << " written " << prevHash << " read " << newHash);
}

void TValidationService::ReportRace(
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
        << "[" << volume.DiskId << "] "
        << newRange << " overlaps with " << oldRange);
}

////////////////////////////////////////////////////////////////////////////////

TVolumePtr TValidationService::MountVolume(
    const TString& diskId,
    const TString& clientId,
    ui32 blockSize,
    TDuration remountTimeout)
{
    with_lock (Lock) {
        TVolumeMap::iterator it;
        bool inserted;

        TVector<char> buffer(blockSize);
        auto zeroBlockDigest = DigestCalculator->Calculate(
            InvalidBlockIndex,
            {
                buffer.data(),
                buffer.size()
            }
        );

        std::tie(it, inserted) = Volumes.emplace(
            diskId,
            std::make_shared<TVolume>(diskId, blockSize, zeroBlockDigest));

        if (inserted) {
            RequestCounters.MountedVolumes->Inc();
            RequestCounters.BlocksCount->Add(it->second->Blocks.size());
        }

        auto now = Now();
        auto& volume = *it->second;
        auto res = volume.ClientIds.emplace(
            clientId,
            std::make_pair(now, remountTimeout));
        if (!res.second) {
            res.first->second = {now, remountTimeout};
        }

        return it->second;
    }
}

void TValidationService::UnmountVolume(
    const TString& diskId,
    const TString& clientId)
{
    with_lock (Lock) {
        auto it = Volumes.find(diskId);
        if (it != Volumes.end()) {
            it->second->ClientIds.erase(clientId);
            if (it->second->ClientIds.empty()) {
                RequestCounters.BlocksCount->Sub(it->second->Blocks.size());
                Volumes.erase(it);
                RequestCounters.MountedVolumes->Dec();
            }
        } else {
            ReportWarning(TStringBuilder()
                << "unknown volume: " << diskId);
        }
    }
}

TVolumePtr TValidationService::GetVolume(
    const TString& diskId,
    const TString& clientId)
{
    with_lock (Lock) {
        auto it = Volumes.find(diskId);
        if (it == Volumes.end()) {
            ReportWarning(TStringBuilder()
                << "unknown volume: " << diskId);
            return {};
        }

        auto clientIt = it->second->ClientIds.find(clientId);
        if (clientIt == it->second->ClientIds.end()) {
            ReportError(TStringBuilder()
                << "unknown client: " << clientId
                << " for volume: " << diskId);
        } else {
            auto deadline = clientIt->second.first + clientIt->second.second;
            if (deadline < Now()) {
                ReportError(TStringBuilder()
                    << "request after automatic unmount. client: " << clientId
                    << " for volume: " << diskId);
            }
            clientIt->second.first = Now();
        }

        return it->second;
    }
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<TOperationRange> TValidationService::CreateRange(
    ui64 startIndex,
    ui32 blocksCount,
    EOperation op)
{
    if (startIndex > MaxBlockIndex) {
        // ignore range completely
        return Nothing();
    }

    ui64 endIndex = startIndex + blocksCount - 1;
    if (endIndex > MaxBlockIndex) {
        // trim range
        endIndex = MaxBlockIndex;
    }

    return TOperationRange {
        GenerateRequestId(),
        startIndex,
        endIndex,
        op
    };
}

void TValidationService::EnterRange(
    TVolume& volume,
    TOperationRange range)
{
    for (auto& r: volume.Requests) {
        if (range.Overlaps(r)) {
            if (range.Op == EOperation::Read && r.Op == EOperation::Read) {
                // read/read race is ok
                continue;
            }
            ReportRace(volume, range, r);
            range.RaceDetected = true;

            if (r.Op != EOperation::Read && range.Op != EOperation::Read) {
                // in a write/write race both requests are affected
                r.RaceDetected = true;
            }
        }
    }

    volume.Requests.push_back(range);
}

void TValidationService::LeaveRange(
    TVolume& volume,
    TRequestsInFlight::iterator it)
{
    volume.Requests.erase(it);
}

void TValidationService::OnError(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        auto it = std::find(volume.Requests.begin(), volume.Requests.end(), range);
        Y_ABORT_UNLESS(it != volume.Requests.end());
        LeaveRange(volume, it);
    }
}

TVector<ui64> TValidationService::PrepareRead(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        EnterRange(volume, range);

        TVector<ui64> result(Reserve(range.Size()));
        for (ui32 i = 0; i < range.Size(); ++i) {
            auto it = volume.Blocks.find(range.Begin + i);
            if (it != volume.Blocks.end()) {
                result.push_back(it->second);
            } else {
                // if block is absent we assume it's crc is zero
                result.push_back(0);
            }
        }

        return result;
    }
}

void TValidationService::CompleteRead(
    TVolume& volume,
    const TOperationRange& range,
    const TVector<ui64>& blocksWritten,
    const TVector<ui64>& blocksRead)
{
    with_lock (volume.Lock) {
        auto it = std::find(volume.Requests.begin(), volume.Requests.end(), range);
        Y_ABORT_UNLESS(it != volume.Requests.end());

        if (blocksWritten.size() != blocksRead.size()) {
            ReportIncompleteRead(
                volume,
                blocksWritten.size(),
                blocksRead.size());
        }

        if (!it->RaceDetected) {
            for (ui32 i = 0; i < Min(blocksWritten.size(), blocksRead.size()); ++i) {
                if (!blocksWritten[i]) {
                    // did not see this block before -> skip
                    continue;
                }

                if (blocksWritten[i] != blocksRead[i]) {
                    ReportInconsistentRead(
                        volume,
                        it->Begin + i,
                        blocksWritten[i],
                        blocksRead[i]);
                }
            }
        }

        LeaveRange(volume, it);
    }
}

void TValidationService::PrepareWrite(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        EnterRange(volume, range);
    }
}

void TValidationService::CompleteWrite(
    TVolume& volume,
    const TOperationRange& range,
    const TVector<ui64>& blocks)
{
    with_lock (volume.Lock) {
        auto it = std::find(volume.Requests.begin(), volume.Requests.end(), range);
        Y_ABORT_UNLESS(it != volume.Requests.end());

        auto oldSize = volume.Blocks.size();

        for (ui32 i = 0; i < blocks.size(); ++i) {
            if (it->RaceDetected) {
                volume.Blocks[it->Begin + i] = 0;
            } else {
                volume.Blocks[it->Begin + i] = blocks[i];
            }
        }

        RequestCounters.BlocksCount->Add(volume.Blocks.size() - oldSize);
        LeaveRange(volume, it);
    }
}

void TValidationService::PrepareZero(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        EnterRange(volume, range);
    }
}

void TValidationService::CompleteZero(
    TVolume& volume,
    const TOperationRange& range)
{
    with_lock (volume.Lock) {
        auto it = std::find(volume.Requests.begin(), volume.Requests.end(), range);
        Y_ABORT_UNLESS(it != volume.Requests.end());

        auto oldSize = volume.Blocks.size();

        for (ui32 i = 0; i < it->Size(); ++i) {
            volume.Blocks[it->Begin + i] = 0;
        }

        RequestCounters.BlocksCount->Add(volume.Blocks.size() - oldSize);
        LeaveRange(volume, it);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TValidationService::HandleRequest(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TMountVolumeRequest> request,
    TFuture<NProto::TMountVolumeResponse>& response)
{
    RequestCounters.MountRequests->Inc();

    auto diskId = request->GetDiskId();
    auto clientId = request->GetHeaders().GetClientId();
    response = Service->MountVolume(std::move(ctx), std::move(request)).Subscribe(
            [=, this] (const auto& future) {
            if (!IsRequestFailed(future)) {
                const auto& response = future.GetValue();
                auto timeout = InactiveClientsTimeout * 1.05;
                auto volume = MountVolume(
                    diskId,
                    clientId,
                    response.GetVolume().GetBlockSize(),
                    timeout);
            }
        });
    return true;
}

bool TValidationService::HandleRequest(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TUnmountVolumeRequest> request,
    TFuture<NProto::TUnmountVolumeResponse>& response)
{
    RequestCounters.UnmountRequests->Inc();

    auto diskId = request->GetDiskId();
    auto clientId = request->GetHeaders().GetClientId();
    response = Service->UnmountVolume(std::move(ctx), std::move(request)).Subscribe(
        [=, this] (const auto& future) {
            if (!IsRequestFailed(future)) {
                UnmountVolume(diskId, clientId);
            }
        });
    return true;
}

bool TValidationService::HandleRequest(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TReadBlocksRequest> request,
    TFuture<NProto::TReadBlocksResponse>& response)
{
    RequestCounters.ReadRequests->Inc();

    auto& diskId = request->GetDiskId();
    auto& clientId = request->GetHeaders().GetClientId();
    auto volume = GetVolume(diskId, clientId);
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    auto range = CreateRange(
        request->GetStartIndex(),
        request->GetBlocksCount(),
        EOperation::Read);

    if (!range) {
        // ignore invalid range
        return false;
    }

    const auto& checkpointId = request->GetCheckpointId();
    if (checkpointId) {
        // we only track last block versions
        return false;
    }

    const auto zeroBlockDigest = AtomicGet(volume->ZeroBlockDigest);
    auto blocks = PrepareRead(*volume, *range);
    Y_ABORT_UNLESS(blocks.size() == range->Size());

    response = Service->ReadBlocks(std::move(ctx), std::move(request)).Subscribe(
        [=, this, blocks_ = std::move(blocks)] (const auto& future) {
            if (!IsRequestFailed(future)) {
                const auto& response = future.GetValue();

                auto sgListOrError = GetSgList(response, volume->BlockSize);
                if (HasError(sgListOrError)) {
                    OnError(*volume, *range);
                    return;
                }

                auto blocksRead = CalculateBlocksDigest(
                    sgListOrError.GetResult(),
                    *DigestCalculator,
                    volume->BlockSize,
                    range->Begin,
                    range->Size(),
                    zeroBlockDigest
                );

                CompleteRead(*volume, *range, blocks_, blocksRead);
            } else {
                OnError(*volume, *range);
            }
        });
    return true;
}

bool TValidationService::HandleRequest(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request,
    TFuture<NProto::TReadBlocksLocalResponse>& response)
{
    RequestCounters.ReadRequests->Inc();

    auto& diskId = request->GetDiskId();
    auto& clientId = request->GetHeaders().GetClientId();
    auto volume = GetVolume(diskId, clientId);
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    auto range = CreateRange(
        request->GetStartIndex(),
        request->GetBlocksCount(),
        EOperation::Read);

    if (!range) {
        // ignore invalid range
        return false;
    }

    const auto& checkpointId = request->GetCheckpointId();
    if (checkpointId) {
        // we only track last block versions
        return false;
    }

    const auto zeroBlockDigest = AtomicGet(volume->ZeroBlockDigest);
    auto blocks = PrepareRead(*volume, *range);
    Y_ABORT_UNLESS(blocks.size() == range->Size());

    response = Service->ReadBlocksLocal(std::move(ctx), request).Subscribe(
        [=, this, blocks_ = std::move(blocks)] (const auto& future) {
            if (!IsRequestFailed(future)) {
                auto guard = request->Sglist.Acquire();

                if (guard) {
                    auto blocksRead = CalculateBlocksDigest(
                        guard.Get(),
                        *DigestCalculator,
                        volume->BlockSize,
                        range->Begin,
                        range->Size(),
                        zeroBlockDigest
                    );

                    CompleteRead(*volume, *range, blocks_, blocksRead);
                    return;
                }
            }

            OnError(*volume, *range);
        });

    return true;
}

bool TValidationService::HandleRequest(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TWriteBlocksRequest> request,
    TFuture<NProto::TWriteBlocksResponse>& response)
{
    RequestCounters.WriteRequests->Inc();

    auto& diskId = request->GetDiskId();
    auto& clientId = request->GetHeaders().GetClientId();
    auto volume = GetVolume(diskId, clientId);
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    auto sgListOrError = SgListNormalize(
        GetSgList(*request),
        volume->BlockSize);

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
        volume->BlockSize,
        range->Begin,
        range->Size(),
        AtomicGet(volume->ZeroBlockDigest)
    );

    PrepareWrite(*volume, *range);

    response = Service->WriteBlocks(std::move(ctx), std::move(request)).Subscribe(
        [=, this, blocks_ = std::move(blocks)] (const auto& future) {
            if (!IsRequestFailed(future)) {
                CompleteWrite(*volume, *range, blocks_);
            } else {
                OnError(*volume, *range);
            }
        });
    return true;
}

bool TValidationService::HandleRequest(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request,
    TFuture<NProto::TWriteBlocksLocalResponse>& response)
{
    RequestCounters.WriteRequests->Inc();

    auto& diskId = request->GetDiskId();
    auto& clientId = request->GetHeaders().GetClientId();
    auto volume = GetVolume(diskId, clientId);
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    auto range = CreateRange(
        request->GetStartIndex(),
        request->BlocksCount,
        EOperation::Write);

    if (!range) {
        // ignore invalid range
        return false;
    }

    auto guard = request->Sglist.Acquire();
    Y_ABORT_UNLESS(guard);

    auto blocks = CalculateBlocksDigest(
        guard.Get(),
        *DigestCalculator,
        volume->BlockSize,
        range->Begin,
        range->Size(),
        AtomicGet(volume->ZeroBlockDigest)
    );

    PrepareWrite(*volume, *range);

    response = Service->WriteBlocksLocal(std::move(ctx), std::move(request))
        .Subscribe([=, this, blocks_ = std::move(blocks)] (const auto& future) {
            if (!IsRequestFailed(future)) {
                CompleteWrite(*volume, *range, blocks_);
            } else {
                OnError(*volume, *range);
            }
        });
    return true;
}

bool TValidationService::HandleRequest(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TZeroBlocksRequest> request,
    TFuture<NProto::TZeroBlocksResponse>& response)
{
    RequestCounters.WriteRequests->Inc();

    auto& diskId = request->GetDiskId();
    auto& clientId = request->GetHeaders().GetClientId();
    auto volume = GetVolume(diskId, clientId);
    if (!volume) {
        // ignore unknown volume
        return false;
    }

    auto range = CreateRange(
        request->GetStartIndex(),
        request->GetBlocksCount(),
        EOperation::Zero);

    if (!range) {
        // ignore invalid range
        return false;
    }

    PrepareZero(*volume, *range);

    response = Service->ZeroBlocks(std::move(ctx), std::move(request)).Subscribe(
        [=, this] (const auto& future) {
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

IBlockStorePtr CreateValidationService(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr service,
    IBlockDigestCalculatorPtr digestCalculator,
    TDuration inactiveClientTimeout,
    IValidationCallbackPtr callback)
{
    return std::make_unique<TValidationService>(
        std::move(logging),
        std::move(monitoring),
        std::move(service),
        std::move(digestCalculator),
        inactiveClientTimeout,
        std::move(callback));
}

}   // namespace NCloud::NBlockStore::NServer

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NCloud::NBlockStore::NServer::TOperationRange>(
    IOutputStream& out,
    const NCloud::NBlockStore::NServer::TOperationRange& range)
{
    out << NCloud::NBlockStore::NServer::GetOperationString(range.Op)
        << "(" << range.Begin << ", " << range.End << ")";
}
