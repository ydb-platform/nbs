#include "profile_log.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TRanges =
    google::protobuf::RepeatedPtrField<NProto::TProfileLogBlockRange>;

TString RangesToString(const TRanges& ranges)
{
    TStringBuilder sb;

    for (int i = 0; i < ranges.size(); ++i) {
        if (i) {
            sb << " ";
        }

        sb << ranges[i].GetNodeId()
            << "," << ranges[i].GetHandle()
            << "," << ranges[i].GetOffset()
            << "," << ranges[i].GetBytes();
    }

    return std::move(sb);
}

TString NodeInfoToString(const NProto::TProfileLogNodeInfo& nodeInfo)
{
    return TStringBuilder() << nodeInfo.GetParentNodeId()
        << "," << nodeInfo.GetNodeName()
        << "," << nodeInfo.GetNewParentNodeId()
        << "," << nodeInfo.GetNewNodeName()
        << "," << nodeInfo.GetFlags()
        << "," << nodeInfo.GetMode()
        << "," << nodeInfo.GetNodeId()
        << "," << nodeInfo.GetHandle()
        << "," << nodeInfo.GetSize()
        << "," << nodeInfo.GetType();
}

TString LockInfoToString(const NProto::TProfileLogLockInfo& lockInfo)
{
    return TStringBuilder() << lockInfo.GetNodeId()
        << "," << lockInfo.GetHandle()
        << "," << lockInfo.GetOwner()
        << "," << lockInfo.GetOffset()
        << "," << lockInfo.GetLength()
        << "," << lockInfo.GetType()
        << "," << lockInfo.GetConflictedOwner()
        << "," << lockInfo.GetConflictedOffset()
        << "," << lockInfo.GetConflictedLength();
}

////////////////////////////////////////////////////////////////////////////////

struct TEventProcessor
    : TProtobufEventProcessor
{
    TVector<TString> FlatMessages;
    TVector<ui32> MessageCountByRecord;

    void Clear()
    {
        FlatMessages.clear();
        MessageCountByRecord.clear();
    }

    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        Y_UNUSED(out);

        auto message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message) {
            for (const auto& r: message->GetRequests()) {
                FlatMessages.push_back(
                    TStringBuilder() << message->GetFileSystemId()
                        << "\t" << r.GetTimestampMcs()
                        << "\t" << r.GetRequestType()
                        << "\t" << r.GetDurationMcs()
                        << "\t" << r.GetErrorCode()
                );
                if (r.HasNodeInfo()) {
                    FlatMessages.back() += TStringBuilder() <<
                        "\t" << NodeInfoToString(r.GetNodeInfo());
                }
                if (r.HasLockInfo()) {
                    FlatMessages.back() += TStringBuilder() <<
                        "\t" << LockInfoToString(r.GetLockInfo());
                }
                if (r.RangesSize() > 0) {
                    FlatMessages.back() += TStringBuilder() <<
                        "\t" << RangesToString(r.GetRanges());
                }
            }

            MessageCountByRecord.push_back(message->GetRequests().size());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
    : public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;
    const TFsPath ProfilePath = TempDir.Path() / "profile.log";

    ITimerPtr Timer;
    std::shared_ptr<TTestScheduler> Scheduler;
    TProfileLogSettings Settings;
    IProfileLogPtr ProfileLog;
    TEventProcessor EventProcessor;

    TEnv()
        : Timer(CreateWallClockTimer())
        , Scheduler(new TTestScheduler())
        , Settings{ProfilePath.c_str(), TDuration::Seconds(1), 0, 0}
        , ProfileLog(CreateProfileLog(Settings, Timer, Scheduler))
    {}

    TEnv(ui64 maxFlushRecords, ui64 maxFrameFlushRecords)
        : Timer(CreateWallClockTimer())
        , Scheduler(new TTestScheduler())
        , Settings(
              ProfilePath.c_str(),
              TDuration::Seconds(1),
              maxFlushRecords,
              maxFrameFlushRecords)
        , ProfileLog(CreateProfileLog(Settings, Timer, Scheduler))
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        ProfileLog->Start();
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {
        ProfileLog->Stop();
        ProfilePath.DeleteIfExists();
    }

    void ProcessLog(bool runScheduler = true)
    {
        if (runScheduler) {
            Scheduler->RunAllScheduledTasks();
        }

        EventProcessor.Clear();
        const char* argv[] = {"foo", Settings.FilePath.c_str()};
        IterateEventLog(NEvClass::Factory(), &EventProcessor, 2, argv);
        Sort(EventProcessor.FlatMessages);
    }
};

struct TEnvWithLimits
    : public TEnv
{
    TEnvWithLimits(ui64 maxFlushRecords, ui64 maxFrameFlushRecords)
        : TEnv(maxFlushRecords, maxFrameFlushRecords)
    {
        ProfileLog->Start();
    }

    ~TEnvWithLimits()
    {
        ProfileLog->Stop();
        ProfilePath.DeleteIfExists();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestInfoBuilder
{
    NProto::TProfileLogRequestInfo R;

    auto& SetTimestamp(TInstant t)
    {
        R.SetTimestampMcs(t.MicroSeconds());
        return *this;
    }

    auto& SetDuration(TDuration d)
    {
        R.SetDurationMcs(d.MicroSeconds());
        return *this;
    }

    auto& SetRequestType(ui32 t)
    {
        R.SetRequestType(t);
        return *this;
    }

    auto& SetError(ui32 errorCode)
    {
        R.SetErrorCode(errorCode);
        return *this;
    }

    auto& AddRange(ui64 nodeId, ui64 handle, ui64 offset, ui32 size)
    {
        auto* range = R.AddRanges();
        range->SetNodeId(nodeId);
        range->SetHandle(handle);
        range->SetOffset(offset);
        range->SetBytes(size);
        return *this;
    }

    auto& AddNodeInfo(
        ui64 parentNodeId,
        TString nodeName,
        ui64 newParentNodeId,
        TString newNodeName,
        ui32 flags,
        ui32 mode,
        ui64 nodeId,
        ui64 handle,
        ui64 size,
        ui32 type)
    {
        auto nodeInfo = R.MutableNodeInfo();
        nodeInfo->SetParentNodeId(parentNodeId);
        nodeInfo->SetNodeName(std::move(nodeName));
        nodeInfo->SetNewParentNodeId(newParentNodeId);
        nodeInfo->SetNewNodeName(std::move(newNodeName));
        nodeInfo->SetFlags(flags);
        nodeInfo->SetMode(mode);
        nodeInfo->SetNodeId(nodeId);
        nodeInfo->SetHandle(handle);
        nodeInfo->SetSize(size);
        nodeInfo->SetType(type);
        return *this;
    }

    auto& AddLockInfo(
        ui64 nodeId,
        ui64 handle,
        ui64 owner,
        ui64 offset,
        ui64 length,
        ui32 type,
        ui64 conflictedOwner,
        ui64 conflictedOffset,
        ui64 conflictedLength)
    {
        auto lockInfo = R.MutableLockInfo();
        lockInfo->SetNodeId(nodeId);
        lockInfo->SetHandle(handle);
        lockInfo->SetOwner(owner);
        lockInfo->SetOffset(offset);
        lockInfo->SetLength(length);
        lockInfo->SetType(type);
        lockInfo->SetConflictedOwner(conflictedOwner);
        lockInfo->SetConflictedOffset(conflictedOffset);
        lockInfo->SetConflictedLength(conflictedLength);
        return *this;
    }

    auto Build() const
    {
        return R;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProfileLogTest)
{
    Y_UNIT_TEST_F(TestSmoke, TEnv)
    {
        for (ui32 i = 1; i <= 100'000; ++i) {
            ProfileLog->Write({
                TStringBuilder() << "fs" << (i % 10),
                TRequestInfoBuilder()
                    .SetTimestamp(TInstant::MilliSeconds(i))
                    .SetDuration(TDuration::MilliSeconds(i % 200))
                    .SetRequestType(i % 3)
                    .AddRange(i % 1'000, i % 10'000, i, i * 2)
                    .Build()
            });
        }

        ProcessLog();

        UNIT_ASSERT_VALUES_EQUAL(100'000, EventProcessor.FlatMessages.size());
    }

    Y_UNIT_TEST_F(TestDumpMessages, TEnv)
    {
        ProfileLog->Write({
            "fs1",
            TRequestInfoBuilder()
                .SetTimestamp(TInstant::Seconds(1))
                .SetDuration(TDuration::MilliSeconds(100))
                .SetRequestType(1)
                .SetError(0)
                .AddRange(111, 23, 200, 10)
                .AddRange(111, 23, 300, 5)
                .Build()
        });

        ProfileLog->Write({
            "fs1",
            TRequestInfoBuilder()
                .SetTimestamp(TInstant::Seconds(2))
                .SetDuration(TDuration::MilliSeconds(200))
                .SetRequestType(3)
                .SetError(1)
                .AddRange(6000, 42, 500, 20)
                .Build()
        });

        ProfileLog->Write({
            "fs2",
            TRequestInfoBuilder()
                .SetTimestamp(TInstant::Seconds(3))
                .SetDuration(TDuration::MilliSeconds(400))
                .SetRequestType(7)
                .SetError(0)
                .Build()
        });

        ProfileLog->Write({
            "fs2",
            TRequestInfoBuilder()
                .SetTimestamp(TInstant::Seconds(4))
                .SetDuration(TDuration::MilliSeconds(800))
                .SetRequestType(11)
                .SetError(1)
                .AddNodeInfo(1, "node", 2, "new_node", 3, 7, 12, 123, 32, 2)
                .Build()
        });

        ProfileLog->Write({
            "fs3",
            TRequestInfoBuilder()
                .SetTimestamp(TInstant::Seconds(5))
                .SetDuration(TDuration::MilliSeconds(1'600))
                .SetRequestType(11)
                .SetError(1)
                .AddLockInfo(1, 2, 3, 7, 12, 123, 32, 33, 34)
                .Build()
        });


        ProcessLog();

        UNIT_ASSERT_VALUES_EQUAL(5, EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "fs1\t1000000\t1\t100000\t0\t111,23,200,10 111,23,300,5",
            EventProcessor.FlatMessages[0]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "fs1\t2000000\t3\t200000\t1\t6000,42,500,20",
            EventProcessor.FlatMessages[1]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "fs2\t3000000\t7\t400000\t0",
            EventProcessor.FlatMessages[2]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "fs2\t4000000\t11\t800000\t1\t1,node,2,new_node,3,7,12,123,32,2",
            EventProcessor.FlatMessages[3]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "fs3\t5000000\t11\t1600000\t1\t1,2,3,7,12,123,32,33,34",
            EventProcessor.FlatMessages[4]
        );

        ProfileLog->Write({
            "fs3",
            TRequestInfoBuilder()
                .SetTimestamp(TInstant::Seconds(6))
                .SetDuration(TDuration::MilliSeconds(300))
                .SetRequestType(4)
                .SetError(0)
                .Build()
        });

        ProcessLog();

        UNIT_ASSERT_VALUES_EQUAL(6, EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "fs3\t6000000\t4\t300000\t0",
            EventProcessor.FlatMessages[5]
        );

        // Test flush on destruct
        ProfileLog->Write(
            {"fs3",
             TRequestInfoBuilder()
                 .SetTimestamp(TInstant::Seconds(9))
                 .SetDuration(TDuration::MilliSeconds(100))
                 .SetRequestType(4)
                 .SetError(0)
                 .Build()});

        ProfileLog = CreateProfileLogStub();
        ProcessLog(false);

        UNIT_ASSERT_VALUES_EQUAL(7, EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "fs3\t9000000\t4\t100000\t0",
            EventProcessor.FlatMessages[6]);
    }

    Y_UNIT_TEST(TestRecordLimits)
    {
        ui64 maxFlushRecords = 10;
        ui64 maxFrameFlushRecords = 2;

        TEnvWithLimits env{maxFlushRecords, maxFrameFlushRecords};

        for (ui64 i = 0; i < maxFlushRecords + 10; i++) {
            env.ProfileLog->Write(
                {"fs1",
                 TRequestInfoBuilder()
                     .SetTimestamp(TInstant::Seconds(1))
                     .SetDuration(TDuration::MilliSeconds(100))
                     .SetRequestType(1)
                     .SetError(0)
                     .AddRange(111, 23, 200, 10)
                     .AddRange(111, 23, 300, 5)
                     .Build()});
        }

        env.ProcessLog();

        // check all messages flushed
        UNIT_ASSERT_VALUES_EQUAL(
            maxFlushRecords,
            env.EventProcessor.FlatMessages.size());

        // check maxFlushRecords / maxFrameFlushRecords records were written
        UNIT_ASSERT_VALUES_EQUAL(
            maxFlushRecords / maxFrameFlushRecords,
            env.EventProcessor.MessageCountByRecord.size());

        // check each record contains maxFrameFlushRecords records
        for (ui64 i = 0; i < maxFlushRecords / maxFrameFlushRecords; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                maxFrameFlushRecords,
                env.EventProcessor.MessageCountByRecord[i]);
        }
    }
}

}   // namespace NCloud::NFileStore
