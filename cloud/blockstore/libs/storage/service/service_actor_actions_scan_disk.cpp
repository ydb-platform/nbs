#include "service_actor.h"

#include "scan_disk_batch_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <google/protobuf/util/json_util.h>

#include <optional>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBatchSize = 100;
constexpr ui32 DefaultMaxRetries = 5;
constexpr auto DefaultRetryTimeout = TDuration::Seconds(1);
constexpr auto DefaultUnavailableTimeout = TDuration::Minutes(10);
constexpr auto DefaultPollTimeout = TDuration::Minutes(10);
constexpr ui32 DefaultMaxPendingBlobResults = 1000;

constexpr auto MaxRetryDelay = TDuration::Minutes(1);

using TScanDiskRequest = NPrivateProto::TScanDiskRequest;
using TBlobResult = NPrivateProto::TScanDiskBlobResult;
using TBlobToRetry = NPrivateProto::TScanDiskBlobToRetry;
using TBatchRequest = NPrivateProto::TScanDiskBatchRequest;
using TBatchResponse = NPrivateProto::TScanDiskBatchResponse;
using TRecord = NPrivateProto::TScanDiskRecord;
using TStatus = NPrivateProto::TGetScanDiskStatusResponse;

////////////////////////////////////////////////////////////////////////////////

// clang-format off

// The counters a batch returns.
#define SCAN_DISK_BATCH_COUNTERS(xxx)                                          \
    xxx(DeadBlobs)                                                             \
    xxx(CheckedBlobs)                                                          \
    xxx(ReplicatingBlobs)                                                      \
    xxx(DegradedBlobs)                                                         \
    xxx(BrokenBlobs)                                                           \
    xxx(ChecksumVerifiedBlobs)                                                 \
    xxx(ChecksumMismatchBlobs)                                                 \
    xxx(DeletedDuringScan)                                                     \
// SCAN_DISK_BATCH_COUNTERS

// The counters of the blobs the scan gives up on after the last retry.
#define SCAN_DISK_GIVEN_UP_COUNTERS(xxx)                                       \
    xxx(UncheckedBlobs)                                                        \
    xxx(ReadFailedBlobs)                                                       \
    xxx(UnconfirmedBlobs)                                                      \
    xxx(ChecksumNotVerifiedBlobs)                                              \
// SCAN_DISK_GIVEN_UP_COUNTERS

#define SCAN_DISK_COUNTERS(xxx)                                                \
    SCAN_DISK_BATCH_COUNTERS(xxx)                                              \
    SCAN_DISK_GIVEN_UP_COUNTERS(xxx)                                           \
// SCAN_DISK_COUNTERS

// clang-format on

struct TScanCounters
{
#define SCAN_DISK_DECLARE_COUNTER(name) ui64 name = 0;
    SCAN_DISK_COUNTERS(SCAN_DISK_DECLARE_COUNTER)
#undef SCAN_DISK_DECLARE_COUNTER

    void Add(const TBatchResponse& response)
    {
#define SCAN_DISK_ADD_COUNTER(name) name += response.Get##name();
        SCAN_DISK_BATCH_COUNTERS(SCAN_DISK_ADD_COUNTER)
#undef SCAN_DISK_ADD_COUNTER
    }

    void Add(const TScanCounters& other)
    {
#define SCAN_DISK_ADD_COUNTER(name) name += other.name;
        SCAN_DISK_COUNTERS(SCAN_DISK_ADD_COUNTER)
#undef SCAN_DISK_ADD_COUNTER
    }

    void AddGivenUpBlob(const TBlobResult& result)
    {
        switch (result.GetVerdict()) {
            case TBlobResult::UNCHECKED:
                ++UncheckedBlobs;
                break;
            case TBlobResult::READ_FAILED:
                ++CheckedBlobs;
                ++ReadFailedBlobs;
                break;
            case TBlobResult::REPLICATING:
                ++CheckedBlobs;
                ++ReplicatingBlobs;
                break;
            case TBlobResult::DEGRADED:
                ++CheckedBlobs;
                ++DegradedBlobs;
                break;
            case TBlobResult::BROKEN:
                ++CheckedBlobs;
                ++BrokenBlobs;
                break;
            case TBlobResult::CHECKSUM_MISMATCH:
                ++CheckedBlobs;
                ++ChecksumMismatchBlobs;
                break;
            default:
                ++CheckedBlobs;
                break;
        }

        if (result.GetUnconfirmed()) {
            ++UnconfirmedBlobs;
        }
        if (result.GetChecksumsNotVerified()) {
            ++ChecksumNotVerifiedBlobs;
        }
    }

    NPrivateProto::TScanDiskCounters ToProto() const
    {
        NPrivateProto::TScanDiskCounters counters;
#define SCAN_DISK_SET_COUNTER(name) counters.Set##name(name);
        SCAN_DISK_COUNTERS(SCAN_DISK_SET_COUNTER)
#undef SCAN_DISK_SET_COUNTER
        return counters;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool IsWrittenWhenGivenUp(TBlobResult::EVerdict verdict)
{
    // A healthy blob whose checksums couldn't be verified is only counted.
    return verdict != TBlobResult::HEALTHY &&
           verdict != TBlobResult::REPLICATING;
}

template <typename T>
T ValueOrDefault(T value, T defaultValue)
{
    return value ? value : defaultValue;
}

TDuration MillisecondsOrDefault(ui32 milliseconds, TDuration defaultValue)
{
    return milliseconds ? TDuration::MilliSeconds(milliseconds) : defaultValue;
}

////////////////////////////////////////////////////////////////////////////////

struct TPartitionScan
{
    ui32 PartitionIndex = 0;
    TString Cursor;
    ui64 ScanCommitId = 0;
    bool IsCompleted = false;
    NProto::TError Error;
    TScanCounters Counters;
};

// One listing and the rechecks of the blobs it left without a final result.
struct TBatch
{
    TString NextCursor;
    bool IsCompleted = false;
    TScanCounters Counters;
    google::protobuf::RepeatedPtrField<TBlobResult> BlobResults;
    google::protobuf::RepeatedPtrField<TBlobToRetry> BlobsToRetry;
    ui32 Retries = 0;
    TDuration RetryDelay;
};

////////////////////////////////////////////////////////////////////////////////

// Reports itself to the service when it starts and stops, because
// HandleExecuteAction keeps no ids of the actors it registers.
class TScanDiskActor final: public TActorBootstrapped<TScanDiskActor>
{
private:
    enum EState
    {
        Starting,
        // A batch actor is running.
        Running,
        // A timer sends the next batch.
        WaitingToRetry,
        // The unacknowledged records hold too many blob results.
        Paused,
        // Every partition has finished.
        Completed,
    };

    enum EWakeupTag : ui64
    {
        RetryTag = 1,
        PollTimeoutTag = 2,
    };

    const TRequestInfoPtr StartRequestInfo;
    const TString Input;
    const TString OperationId;

    TScanDiskRequest Request;
    ui32 BatchSize = 0;
    ui32 MaxRetries = 0;
    TDuration RetryTimeout;
    TDuration UnavailableTimeout;
    TDuration PollTimeout;
    ui32 MaxPendingBlobResults = 0;
    ui64 MaxCommitId = 0;

    EState State = Starting;

    TVector<TPartitionScan> Partitions;
    ui32 CurrentPartition = 0;

    std::optional<TBatch> Batch;
    TBatchRequest BatchRequest;
    TInstant BatchRequestStarted;
    TDuration BatchRequestRetryDelay;

    TDeque<TRecord> Records;
    ui64 LastRecordId = 0;
    ui64 PendingBlobResults = 0;

    TInstant LastPollTime;

public:
    TScanDiskActor(
        TRequestInfoPtr startRequestInfo,
        TString input,
        TString operationId);

    void Bootstrap(const TActorContext& ctx);

private:
    NProto::TError ParseRequest();
    NProto::TError CheckPartitionFields(ui32 partitionCount) const;

    void ReplyToStart(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);
    void FailStartAndStop(const TActorContext& ctx, NProto::TError error);

    TBatchRequest CreateBatchRequest(const TPartitionScan& partition) const;

    void ScanNextBatch(const TActorContext& ctx);
    void StartBatchRequest(
        const TActorContext& ctx,
        TBatchRequest request,
        TDuration delay);
    void SendBatchRequest(const TActorContext& ctx);
    void ScheduleRetry(const TActorContext& ctx, TDuration delay);
    void HandleBatchError(const TActorContext& ctx, NProto::TError error);
    void FinishBatch(const TActorContext& ctx);
    void FailPartition(const TActorContext& ctx, NProto::TError error);
    void FinishPartition(const TActorContext& ctx);
    void LogPartitionStart(const TActorContext& ctx) const;
    void AddRecord(TRecord record);

    TStatus MakeStatus() const;

    void SchedulePollTimeout(const TActorContext& ctx, TDuration delay);
    void HandlePollTimeout(const TActorContext& ctx);

    void Stop(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleStatVolumeResponse(
        const TEvService::TEvStatVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleScanDiskBatchResponse(
        const TEvServicePrivate::TEvScanDiskBatchResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleScanDiskStatusRequest(
        const TEvServicePrivate::TEvScanDiskStatusRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskActor::TScanDiskActor(
    TRequestInfoPtr startRequestInfo,
    TString input,
    TString operationId)
    : StartRequestInfo(std::move(startRequestInfo))
    , Input(std::move(input))
    , OperationId(std::move(operationId))
{}

void TScanDiskActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    NCloud::Send(
        ctx,
        MakeStorageServiceId(),
        std::make_unique<TEvServicePrivate::TEvScanDiskStarted>(OperationId));

    if (auto error = ParseRequest(); HasError(error)) {
        FailStartAndStop(ctx, std::move(error));
        return;
    }

    // Also bounds the wait for StatVolume.
    LastPollTime = ctx.Now();
    SchedulePollTimeout(ctx, PollTimeout);

    auto request = std::make_unique<TEvService::TEvStatVolumeRequest>(
        StartRequestInfo->CallContext);
    request->Record.SetDiskId(Request.GetDiskId());
    NCloud::Send(ctx, MakeVolumeProxyServiceId(), std::move(request));
}

NProto::TError TScanDiskActor::ParseRequest()
{
    const auto status =
        google::protobuf::util::JsonStringToMessage(Input, &Request);
    if (!status.ok()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Failed to parse input as TScanDiskRequest: "
                             << status.ToString());
    }

    if (!Request.GetDiskId()) {
        return MakeError(E_ARGUMENT, "DiskId is required");
    }

    MaxCommitId = ValueOrDefault(Request.GetMaxCommitId(), Max<ui64>());
    if (Request.GetMinCommitId() > MaxCommitId) {
        return MakeError(E_ARGUMENT, "MinCommitId is above MaxCommitId");
    }

    // The cursor already says where the scan starts.
    if (Request.GetMinCommitId() && Request.GetCursor()) {
        return MakeError(E_ARGUMENT, "MinCommitId can't be used with Cursor");
    }

    BatchSize = ValueOrDefault(Request.GetBatchSize(), DefaultBatchSize);
    MaxRetries = ValueOrDefault(Request.GetMaxRetries(), DefaultMaxRetries);
    RetryTimeout =
        MillisecondsOrDefault(Request.GetRetryTimeoutMs(), DefaultRetryTimeout);
    UnavailableTimeout = MillisecondsOrDefault(
        Request.GetUnavailableTimeoutMs(),
        DefaultUnavailableTimeout);
    PollTimeout =
        MillisecondsOrDefault(Request.GetPollTimeoutMs(), DefaultPollTimeout);
    MaxPendingBlobResults = ValueOrDefault(
        Request.GetMaxPendingBlobResults(),
        DefaultMaxPendingBlobResults);

    return {};
}

NProto::TError TScanDiskActor::CheckPartitionFields(ui32 partitionCount) const
{
    if (Request.HasPartitionIndex()) {
        if (Request.GetPartitionIndex() >= partitionCount) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "PartitionIndex " << Request.GetPartitionIndex()
                    << " is out of range, the disk has " << partitionCount
                    << " partitions");
        }
        return {};
    }

    if (partitionCount == 1) {
        return {};
    }

    // Commit ids and cursors belong to one partition.
    const std::pair<TStringBuf, bool> partitionFields[] = {
        {"MinCommitId", Request.GetMinCommitId() != 0},
        {"MaxCommitId", Request.GetMaxCommitId() != 0},
        {"Cursor", !Request.GetCursor().empty()},
    };
    for (const auto& [name, isSet]: partitionFields) {
        if (isSet) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << name << " needs PartitionIndex on a disk with "
                    << partitionCount << " partitions");
        }
    }

    return {};
}

void TScanDiskActor::ReplyToStart(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        StartRequestInfo->CallContext->LWOrbit,
        "ExecuteAction_scandisk",
        StartRequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *StartRequestInfo, std::move(response));
}

void TScanDiskActor::FailStartAndStop(
    const TActorContext& ctx,
    NProto::TError error)
{
    LOG_WARN(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Scan %s failed to start: %s",
        OperationId.c_str(),
        FormatError(error).c_str());

    ReplyToStart(
        ctx,
        std::make_unique<TEvService::TEvExecuteActionResponse>(
            std::move(error)));
    Stop(ctx);
}

TBatchRequest TScanDiskActor::CreateBatchRequest(
    const TPartitionScan& partition) const
{
    TBatchRequest request;
    request.SetDiskId(Request.GetDiskId());
    request.SetPartitionIndex(partition.PartitionIndex);
    request.SetScanCommitId(partition.ScanCommitId);
    request.SetVerifyChecksums(Request.GetVerifyChecksums());
    request.SetMaxInFlight(Request.GetMaxInFlight());
    request.SetMaxDataInfoSize(Request.GetMaxDataInfoSize());
    return request;
}

void TScanDiskActor::ScanNextBatch(const TActorContext& ctx)
{
    if (CurrentPartition == Partitions.size()) {
        State = Completed;
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Scan %s of disk %s has finished",
            OperationId.c_str(),
            Request.GetDiskId().c_str());
        return;
    }

    if (PendingBlobResults >= MaxPendingBlobResults) {
        State = Paused;
        return;
    }

    const auto& partition = Partitions[CurrentPartition];
    auto request = CreateBatchRequest(partition);
    request.SetCursor(partition.Cursor);
    request.SetMinCommitId(Request.GetMinCommitId());
    request.SetCount(BatchSize);

    StartBatchRequest(ctx, std::move(request), TDuration::Zero());
}

void TScanDiskActor::StartBatchRequest(
    const TActorContext& ctx,
    TBatchRequest request,
    TDuration delay)
{
    BatchRequest = std::move(request);
    BatchRequestStarted = ctx.Now() + delay;
    BatchRequestRetryDelay = RetryTimeout;

    if (delay) {
        ScheduleRetry(ctx, delay);
        return;
    }

    SendBatchRequest(ctx);
}

void TScanDiskActor::SendBatchRequest(const TActorContext& ctx)
{
    State = Running;
    NCloud::Register(ctx, CreateScanDiskBatchActor(SelfId(), BatchRequest));
}

void TScanDiskActor::ScheduleRetry(const TActorContext& ctx, TDuration delay)
{
    State = WaitingToRetry;
    ctx.Schedule(delay, new TEvents::TEvWakeup(RetryTag));
}

void TScanDiskActor::HandleBatchError(
    const TActorContext& ctx,
    NProto::TError error)
{
    const bool isRetriable = GetErrorKind(error) == EErrorKind::ErrorRetriable;
    if (!isRetriable || ctx.Now() - BatchRequestStarted >= UnavailableTimeout) {
        FailPartition(ctx, std::move(error));
        return;
    }

    LOG_WARN(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Scan %s of disk %s retries a batch of partition %u in %s: %s",
        OperationId.c_str(),
        Request.GetDiskId().c_str(),
        Partitions[CurrentPartition].PartitionIndex,
        BatchRequestRetryDelay.ToString().c_str(),
        FormatError(error).c_str());

    const auto delay = BatchRequestRetryDelay;
    BatchRequestRetryDelay = Min(BatchRequestRetryDelay * 2, MaxRetryDelay);
    ScheduleRetry(ctx, delay);
}

void TScanDiskActor::FinishBatch(const TActorContext& ctx)
{
    auto& partition = Partitions[CurrentPartition];

    for (const auto& blob: Batch->BlobsToRetry) {
        const auto& result = blob.GetResult();
        Batch->Counters.AddGivenUpBlob(result);
        if (IsWrittenWhenGivenUp(result.GetVerdict())) {
            *Batch->BlobResults.Add() = result;
        }
    }

    // The next batch starts only when this one has nothing left to retry, so
    // every record is a safe point to resume from.
    TRecord record;
    record.SetPartitionIndex(partition.PartitionIndex);
    record.SetCursor(partition.Cursor);
    record.SetNextCursor(Batch->NextCursor);
    record.SetScanCommitId(partition.ScanCommitId);
    record.SetIsCompleted(Batch->IsCompleted);
    *record.MutableCounters() = Batch->Counters.ToProto();
    record.MutableBlobResults()->Swap(&Batch->BlobResults);
    AddRecord(std::move(record));

    partition.Counters.Add(Batch->Counters);
    partition.Cursor = Batch->NextCursor;
    partition.IsCompleted = Batch->IsCompleted;
    Batch.reset();

    if (partition.IsCompleted) {
        FinishPartition(ctx);
    }

    ScanNextBatch(ctx);
}

void TScanDiskActor::FailPartition(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto& partition = Partitions[CurrentPartition];

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Scan %s of disk %s leaves partition %u incomplete at %s: %s",
        OperationId.c_str(),
        Request.GetDiskId().c_str(),
        partition.PartitionIndex,
        partition.Cursor.Quote().c_str(),
        FormatError(error).c_str());

    // The records before stay valid: a scan started from this cursor redoes
    // only the failed batch.
    TRecord record;
    record.SetPartitionIndex(partition.PartitionIndex);
    record.SetCursor(partition.Cursor);
    record.SetScanCommitId(partition.ScanCommitId);
    *record.MutableError() = error;
    AddRecord(std::move(record));

    partition.Error = std::move(error);
    Batch.reset();

    FinishPartition(ctx);
    ScanNextBatch(ctx);
}

void TScanDiskActor::FinishPartition(const TActorContext& ctx)
{
    ++CurrentPartition;
    if (CurrentPartition < Partitions.size()) {
        LogPartitionStart(ctx);
    }
}

void TScanDiskActor::LogPartitionStart(const TActorContext& ctx) const
{
    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Scan %s of disk %s starts partition %u",
        OperationId.c_str(),
        Request.GetDiskId().c_str(),
        Partitions[CurrentPartition].PartitionIndex);
}

void TScanDiskActor::AddRecord(TRecord record)
{
    record.SetRecordId(++LastRecordId);
    PendingBlobResults += record.BlobResultsSize();
    Records.push_back(std::move(record));
}

TStatus TScanDiskActor::MakeStatus() const
{
    TStatus status;
    for (const auto& record: Records) {
        *status.AddRecords() = record;
    }

    for (const auto& partition: Partitions) {
        auto& partitionStatus = *status.AddPartitions();
        partitionStatus.SetPartitionIndex(partition.PartitionIndex);
        partitionStatus.SetIsCompleted(partition.IsCompleted);
        *partitionStatus.MutableCounters() = partition.Counters.ToProto();
        if (HasError(partition.Error)) {
            *partitionStatus.MutableError() = partition.Error;
            status.AddIncompletePartitions(partition.PartitionIndex);
        }
    }

    status.SetIsCompleted(State == Completed);
    return status;
}

void TScanDiskActor::SchedulePollTimeout(
    const TActorContext& ctx,
    TDuration delay)
{
    ctx.Schedule(delay, new TEvents::TEvWakeup(PollTimeoutTag));
}

void TScanDiskActor::HandlePollTimeout(const TActorContext& ctx)
{
    const auto deadline = LastPollTime + PollTimeout;
    const auto now = ctx.Now();
    if (now < deadline) {
        SchedulePollTimeout(ctx, deadline - now);
        return;
    }

    if (State == Starting) {
        FailStartAndStop(
            ctx,
            MakeError(E_TIMEOUT, "StatVolume didn't reply in time"));
        return;
    }

    LOG_WARN(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Scan %s of disk %s stops: nobody has polled it for %s",
        OperationId.c_str(),
        Request.GetDiskId().c_str(),
        PollTimeout.ToString().c_str());

    Stop(ctx);
}

void TScanDiskActor::Stop(const TActorContext& ctx)
{
    NCloud::Send(
        ctx,
        MakeStorageServiceId(),
        std::make_unique<TEvServicePrivate::TEvScanDiskStopped>(OperationId));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvStatVolumeResponse, HandleStatVolumeResponse);
        HFunc(
            TEvServicePrivate::TEvScanDiskBatchResponse,
            HandleScanDiskBatchResponse);
        HFunc(
            TEvServicePrivate::TEvScanDiskStatusRequest,
            HandleScanDiskStatusRequest);
        HFunc(TEvents::TEvWakeup, HandleWakeup);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TScanDiskActor::HandleStatVolumeResponse(
    const TEvService::TEvStatVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        FailStartAndStop(ctx, msg->GetError());
        return;
    }

    const ui32 partitionCount =
        Max<ui32>(1, msg->Record.GetVolume().GetPartitionsCount());
    if (auto error = CheckPartitionFields(partitionCount); HasError(error)) {
        FailStartAndStop(ctx, std::move(error));
        return;
    }

    auto addPartition = [&](ui32 partitionIndex)
    {
        Partitions.push_back(
            {.PartitionIndex = partitionIndex,
             .Cursor = Request.GetCursor(),
             .ScanCommitId = MaxCommitId});
    };
    if (Request.HasPartitionIndex()) {
        addPartition(Request.GetPartitionIndex());
    } else {
        for (ui32 i = 0; i < partitionCount; ++i) {
            addPartition(i);
        }
    }

    NPrivateProto::TScanDiskResponse response;
    response.SetOperationId(OperationId);
    auto actionResponse =
        std::make_unique<TEvService::TEvExecuteActionResponse>();
    google::protobuf::util::MessageToJsonString(
        response,
        actionResponse->Record.MutableOutput());
    ReplyToStart(ctx, std::move(actionResponse));

    LOG_INFO(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Scan %s of disk %s has started",
        OperationId.c_str(),
        Request.GetDiskId().c_str());

    LastPollTime = ctx.Now();
    LogPartitionStart(ctx);
    ScanNextBatch(ctx);
}

void TScanDiskActor::HandleScanDiskBatchResponse(
    const TEvServicePrivate::TEvScanDiskBatchResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleBatchError(ctx, msg->GetError());
        return;
    }

    auto& response = msg->Record;
    auto& partition = Partitions[CurrentPartition];

    if (!Batch) {
        // Later batches send the bound the partition capped at its last
        // commit, so blobs written after the start stay out of the scan.
        partition.ScanCommitId = response.GetScanCommitId();

        Batch.emplace();
        Batch->NextCursor = response.GetNextCursor();
        Batch->IsCompleted = response.GetIsCompleted();
        Batch->RetryDelay = RetryTimeout;
    }

    Batch->Counters.Add(response);
    for (auto& result: *response.MutableBlobResults()) {
        *Batch->BlobResults.Add() = std::move(result);
    }
    Batch->BlobsToRetry.Swap(response.MutableBlobsToRetry());

    if (Batch->BlobsToRetry.empty() || Batch->Retries == MaxRetries) {
        FinishBatch(ctx);
        return;
    }

    ++Batch->Retries;
    auto recheck = CreateBatchRequest(partition);
    recheck.MutableBlobsToRecheck()->Swap(&Batch->BlobsToRetry);

    const auto delay = Batch->RetryDelay;
    Batch->RetryDelay = Min(Batch->RetryDelay * 2, MaxRetryDelay);
    StartBatchRequest(ctx, std::move(recheck), delay);
}

void TScanDiskActor::HandleScanDiskStatusRequest(
    const TEvServicePrivate::TEvScanDiskStatusRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LastPollTime = ctx.Now();

    const ui64 ackedRecordId = ev->Get()->AckedRecordId;
    while (!Records.empty() && Records.front().GetRecordId() <= ackedRecordId) {
        PendingBlobResults -= Records.front().BlobResultsSize();
        Records.pop_front();
    }

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<TEvServicePrivate::TEvScanDiskStatusResponse>(
            MakeStatus()));

    if (State == Completed && Records.empty()) {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Scan %s of disk %s stops: its last record is acknowledged",
            OperationId.c_str(),
            Request.GetDiskId().c_str());
        Stop(ctx);
        return;
    }

    if (State == Paused) {
        ScanNextBatch(ctx);
    }
}

void TScanDiskActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    switch (ev->Get()->Tag) {
        case RetryTag:
            if (State == WaitingToRetry) {
                SendBatchRequest(ctx);
            }
            break;
        case PollTimeoutTag:
            HandlePollTimeout(ctx);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TGetScanDiskStatusActor final
    : public TActorBootstrapped<TGetScanDiskStatusActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TActorId ScanActorId;
    const ui64 AckedRecordId;

public:
    TGetScanDiskStatusActor(
        TRequestInfoPtr requestInfo,
        TActorId scanActorId,
        ui64 ackedRecordId);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvExecuteActionResponse> response);

private:
    STFUNC(StateWork);

    void HandleScanDiskStatusResponse(
        const TEvServicePrivate::TEvScanDiskStatusResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUndelivered(
        const TEvents::TEvUndelivered::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetScanDiskStatusActor::TGetScanDiskStatusActor(
    TRequestInfoPtr requestInfo,
    TActorId scanActorId,
    ui64 ackedRecordId)
    : RequestInfo(std::move(requestInfo))
    , ScanActorId(scanActorId)
    , AckedRecordId(ackedRecordId)
{}

void TGetScanDiskStatusActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    ctx.Send(
        ScanActorId,
        new TEvServicePrivate::TEvScanDiskStatusRequest(AckedRecordId),
        IEventHandle::FlagTrackDelivery);
}

void TGetScanDiskStatusActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvExecuteActionResponse> response)
{
    LWTRACK(
        ResponseSent_Service,
        RequestInfo->CallContext->LWOrbit,
        "ExecuteAction_getscandiskstatus",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetScanDiskStatusActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvServicePrivate::TEvScanDiskStatusResponse,
            HandleScanDiskStatusResponse);
        HFunc(TEvents::TEvUndelivered, HandleUndelivered);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

void TGetScanDiskStatusActor::HandleScanDiskStatusResponse(
    const TEvServicePrivate::TEvScanDiskStatusResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvExecuteActionResponse>();
    google::protobuf::util::MessageToJsonString(
        ev->Get()->Record,
        response->Record.MutableOutput());

    ReplyAndDie(ctx, std::move(response));
}

void TGetScanDiskStatusActor::HandleUndelivered(
    const TEvents::TEvUndelivered::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    // The scan stopped after the service looked it up.
    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvExecuteActionResponse>(
            MakeError(E_NOT_FOUND, "The scan has stopped")));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IActorPtr> TServiceActor::CreateScanDiskActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    return {std::make_unique<TScanDiskActor>(
        std::move(requestInfo),
        std::move(input),
        CreateGuidAsString())};
}

TResultOrError<IActorPtr> TServiceActor::CreateScanDiskStatusActionActor(
    TRequestInfoPtr requestInfo,
    TString input)
{
    NPrivateProto::TGetScanDiskStatusRequest request;
    if (!google::protobuf::util::JsonStringToMessage(input, &request).ok()) {
        return MakeError(E_ARGUMENT, "Failed to parse input");
    }

    if (!request.GetOperationId()) {
        return MakeError(E_ARGUMENT, "OperationId should be supplied");
    }

    const auto* scanActorId = ScanDiskActors.FindPtr(request.GetOperationId());
    if (!scanActorId) {
        return MakeError(
            E_NOT_FOUND,
            TStringBuilder() << "No scan " << request.GetOperationId().Quote()
                             << " on this host");
    }

    return {std::make_unique<TGetScanDiskStatusActor>(
        std::move(requestInfo),
        *scanActorId,
        request.GetAckedRecordId())};
}

void TServiceActor::HandleScanDiskStarted(
    const TEvServicePrivate::TEvScanDiskStarted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    ScanDiskActors[ev->Get()->OperationId] = ev->Sender;
}

void TServiceActor::HandleScanDiskStopped(
    const TEvServicePrivate::TEvScanDiskStopped::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    ScanDiskActors.erase(ev->Get()->OperationId);
}

}   // namespace NCloud::NBlockStore::NStorage
