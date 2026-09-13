#include "profile_log.h"

#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/eventlog/eventlog.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/thread/lfstack.h>

#include <google/protobuf/arena.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

void AddRange(const TBlockRange64& r, NProto::TProfileLogRequestInfo& ri)
{
    auto* range = ri.AddRanges();
    range->SetBlockIndex(r.Start);
    range->SetBlockCount(r.Size());
}

void AddRangeWithChecksums(
    const TBlockRange64& r,
    const TVector<IProfileLog::TReplicaChecksums>& replicaChecksums,
    NProto::TProfileLogRequestInfo& ri)
{
    auto* range = ri.AddRanges();
    range->SetBlockIndex(r.Start);
    range->SetBlockCount(r.Size());

    for (const auto& replicaChecksums: replicaChecksums) {
        auto* c = range->AddReplicaChecksums();
        c->SetReplicaId(replicaChecksums.ReplicaId);
        c->MutableChecksums()->Assign(
            replicaChecksums.Checksums.begin(),
            replicaChecksums.Checksums.end());
    }
}

template <typename TRequest>
void FillBlockInfos(
    const TRequest& request, NProto::TProfileLogBlockInfoList& ri)
{
    for (const auto& bi: request.BlockInfos) {
        auto* blockInfo = ri.AddBlockInfos();
        blockInfo->SetBlockIndex(bi.BlockIndex);
        blockInfo->SetChecksum(bi.Checksum);
    }
}

NProto::TProfileLogRequestInfo* AddRequest(
    const IProfileLog::TRecord& record,
    const TDuration duration,
    const TDuration postponedTime, NProto::TProfileLogRecord& pb)
{
    auto* ri = pb.AddRequests();
    ri->SetTimestampMcs(record.Ts.MicroSeconds());
    ri->SetDurationMcs(duration.MicroSeconds());
    ri->SetPostponedTimeMcs(postponedTime.MicroSeconds());
    return ri;
}

NProto::TProfileLogRequestInfo* AddRequest(
    const IProfileLog::TRecord& record,
    const TDuration duration, NProto::TProfileLogRecord& pb)
{
    auto* ri = pb.AddRequests();
    ri->SetTimestampMcs(record.Ts.MicroSeconds());
    ri->SetDurationMcs(duration.MicroSeconds());
    return ri;
}

NProto::TProfileLogBlockInfoList* AddBlockInfoList(
    const IProfileLog::TRecord& record, NProto::TProfileLogRecord& pb)
{
    auto* bl = pb.AddBlockInfoLists();
    bl->SetTimestampMcs(record.Ts.MicroSeconds());
    return bl;
}

NProto::TProfileLogBlockCommitIdList* AddBlockCommitIdList(
    const IProfileLog::TRecord& record, NProto::TProfileLogRecord& pb)
{
    auto* bci = pb.AddBlockCommitIdLists();
    bci->SetTimestampMcs(record.Ts.MicroSeconds());
    return bci;
}

NProto::TProfileLogBlobUpdateList* AddBlobUpdateList(
    const IProfileLog::TRecord& record, NProto::TProfileLogRecord& pb)
{
    auto* bul = pb.AddBlobUpdateLists();
    bul->SetTimestampMcs(record.Ts.MicroSeconds());
    return bul;
}

////////////////////////////////////////////////////////////////////////////////

class TProfileLog final
    : public IProfileLog
    , public std::enable_shared_from_this<TProfileLog>
{
private:
    TEventLog EventLog;
    TProfileLogSettings Settings;
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;

    TLockFreeStack<TRecord> Records;

    TMutex FlushLock;

    TAtomic ShouldStop = false;

public:
    TProfileLog(
        TProfileLogSettings settings, ITimerPtr timer, ISchedulerPtr scheduler)
        : EventLog(settings.FilePath, NEvClass::Factory()->CurrentFormat())
        , Settings(std::move(settings))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
    {}

    ~TProfileLog() override;

public:
    void Start() override;
    void Stop() override;

    void Write(TRecord record) override;
    bool Flush() override;

private:
    void ScheduleFlush();
    void DoFlush();
};

TProfileLog::~TProfileLog()
{
    DoFlush();
}

void TProfileLog::Start()
{
    ScheduleFlush();
}

void TProfileLog::Stop()
{
    AtomicSet(ShouldStop, 1);
}

void TProfileLog::Write(TRecord record)
{
    Records.Enqueue(std::move(record));
}

void TProfileLog::ScheduleFlush()
{
    if (AtomicGet(ShouldStop)) {
        return;
    }

    auto weakPtr = weak_from_this();

    Scheduler->Schedule(
        Timer->Now() + Settings.TimeThreshold,
        [weakPtr = std::move(weakPtr)]
        {
            if (auto p = weakPtr.lock()) {
                p->DoFlush();
                p->ScheduleFlush();
            }
        });
}

bool TProfileLog::Flush()
{
    if (AtomicGet(ShouldStop)) {
        return false;
    }

    DoFlush();
    return true;
}

void TProfileLog::DoFlush()
{
    auto guard = Guard(FlushLock);

    struct TGroupedRecords
    {
        THashMap<TString, TVector<TRecord>> ByDisk;

        void push_back(TRecord&& record)
        {
            ByDisk[record.DiskId].push_back(std::move(record));
        }
    } records;

    // Drain directly into disk groups. The previous intermediate vector moved
    // each large record during growth, then allocated another vector of
    // indices. Per-disk dequeue order and the single-consumer synchronization
    // stay intact.
    Records.DequeueAllSingleConsumer(&records);

    if (!records.ByDisk.empty()) {
        TSelfFlushLogFrame logFrame(EventLog);
        for (auto& x: records.ByDisk) {
            // LogEvent serializes into the frame before returning. Release
            // this group's messages together, without retaining other disks.
            google::protobuf::Arena arena;
            auto& pb = *google::protobuf::Arena::CreateMessage<
                NProto::TProfileLogRecord>(&arena);
            pb.SetDiskId(x.first);
            for (const auto& record: x.second) {
                if (auto* rw = std::get_if<TReadWriteRequest>(&record.Request))
                {
                    auto* ri =
                        AddRequest(record, rw->Duration, rw->PostponedTime, pb);
                    AddRange(rw->Range, *ri);
                    ri->SetRequestType(static_cast<ui32>(rw->RequestType));
                    if (rw->RequestTiming.HasData()) {
                        rw->RequestTiming.FillTrace(
                            *ri->MutableRequestTimingTrace());
                    } else if (!rw->RequestTimingJson.empty()) {
                        ri->SetRequestTimingJson(rw->RequestTimingJson);
                    }
                } else if (
                    auto* srw =
                        std::get_if<TSysReadWriteRequest>(&record.Request))
                {
                    auto* ri = AddRequest(record, srw->Duration, pb);
                    for (const auto& r: srw->Ranges) {
                        AddRange(r, *ri);
                    }
                    ri->SetRequestType(static_cast<ui32>(srw->RequestType));
                } else if (
                    auto* srw = std::get_if<TSysReadWriteRequestWithChecksums>(
                        &record.Request))
                {
                    auto* ri = AddRequest(record, srw->Duration, pb);
                    AddRangeWithChecksums(
                        srw->RangeInfo.Range,
                        srw->RangeInfo.ReplicaChecksums, *ri);
                    ri->SetRequestType(static_cast<ui32>(srw->RequestType));
                } else if (
                    auto* misc = std::get_if<TMiscRequest>(&record.Request))
                {
                    auto* ri = AddRequest(record, misc->Duration, pb);
                    ri->SetRequestType(static_cast<ui32>(misc->RequestType));
                } else if (
                    auto* rwbi = std::get_if<TReadWriteRequestBlockInfos>(
                        &record.Request))
                {
                    auto* bl = AddBlockInfoList(record, pb);
                    FillBlockInfos(*rwbi, *bl);
                    bl->SetRequestType(static_cast<ui32>(rwbi->RequestType));
                    bl->SetCommitId(rwbi->CommitId);
                } else if (
                    auto* srwbi = std::get_if<TSysReadWriteRequestBlockInfos>(
                        &record.Request))
                {
                    auto* bl = AddBlockInfoList(record, pb);
                    FillBlockInfos(*srwbi, *bl);
                    bl->SetRequestType(static_cast<ui32>(srwbi->RequestType));
                    bl->SetCommitId(srwbi->CommitId);
                } else if (
                    auto* srwbc =
                        std::get_if<TSysReadWriteRequestBlockCommitIds>(
                            &record.Request))
                {
                    auto* bl = AddBlockCommitIdList(record, pb);
                    for (const auto& bc: srwbc->BlockCommitIds) {
                        auto* blockCommitId = bl->AddBlockCommitIds();
                        blockCommitId->SetBlockIndex(bc.BlockIndex);
                        blockCommitId->SetMinCommitIdOld(bc.MinCommitIdOld);
                        blockCommitId->SetMaxCommitIdOld(bc.MaxCommitIdOld);
                        blockCommitId->SetMinCommitIdNew(bc.MinCommitIdNew);
                        blockCommitId->SetMaxCommitIdNew(bc.MaxCommitIdNew);
                    }
                    bl->SetRequestType(static_cast<ui32>(srwbc->RequestType));
                    bl->SetCommitId(srwbc->CommitId);
                } else if (
                    auto* req =
                        std::get_if<TDescribeBlocksRequest>(&record.Request))
                {
                    auto* ri = AddRequest(record, req->Duration, pb);
                    AddRange(req->Range, *ri);
                    ri->SetRequestType(static_cast<ui32>(req->RequestType));
                } else if (
                    auto* cbu = std::get_if<TCleanupRequestBlobUpdates>(
                        &record.Request))
                {
                    auto* bul = AddBlobUpdateList(record, pb);
                    for (const auto& bu: cbu->BlobUpdates) {
                        auto* blobUpdate = bul->AddBlobUpdates();
                        blobUpdate->SetCommitId(bu.CommitId);
                        auto* br = blobUpdate->MutableBlockRange();
                        br->SetBlockIndex(bu.BlockRange.Start);
                        br->SetBlockCount(bu.BlockRange.Size());
                    }
                    bul->SetCleanupCommitId(cbu->CleanupCommitId);
                }
            }

            logFrame.LogEvent(pb);
        }
    }

    EventLog.Flush();
}

////////////////////////////////////////////////////////////////////////////////

class TProfileLogStub final: public IProfileLog
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    void Write(TRecord record) override
    {
        Y_UNUSED(record);
    }

    bool Flush() override
    {
        return false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IProfileLogPtr CreateProfileLog(
    TProfileLogSettings settings, ITimerPtr timer, ISchedulerPtr scheduler)
{
    return std::make_shared<TProfileLog>(
        std::move(settings), std::move(timer), std::move(scheduler));
}

IProfileLogPtr CreateProfileLogStub()
{
    return std::make_shared<TProfileLogStub>();
}

}   // namespace NCloud::NBlockStore
