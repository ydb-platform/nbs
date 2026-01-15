#include "profile_log.h"

#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEventProcessor
    : TProtobufEventProcessor
{
    TVector<TString> FlatMessages;

    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        Y_UNUSED(out);

        const auto* message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());

        if (message) {
            const TVector<TItemDescriptor> order = GetItemOrder(*message);
            for (const auto& [type, index]: order) {
                TStringStream ss;
                switch (type) {
                    case EItemType::Request: {
                        DumpRequest(*message, index, &ss);
                        FlatMessages.push_back(ss.Str());
                        break;
                    }
                    case EItemType::BlockInfo: {
                        DumpBlockInfoList(*message, index, &ss);
                        FlatMessages.push_back(ss.Str());
                        break;
                    }
                    case EItemType::BlockCommitId: {
                        DumpBlockCommitIdList(*message, index, &ss);
                        FlatMessages.push_back(ss.Str());
                        break;
                    }
                    case EItemType::BlobUpdate: {
                        DumpBlobUpdateList(*message, index, &ss);
                        FlatMessages.push_back(ss.Str());
                        break;
                    }
                    default: {
                        Y_ABORT("unknown item");
                    }
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnv
{
    ITimerPtr Timer;
    std::shared_ptr<TTestScheduler> Scheduler;
    TTempDir TempDir;
    TProfileLogSettings Settings;
    IProfileLogPtr ProfileLog;
    TEventProcessor EventProcessor;

    TEnv()
        : Timer(CreateWallClockTimer())
        , Scheduler(new TTestScheduler())
        , Settings{TempDir.Path() / "profile.log", TDuration::Seconds(1)}
        , ProfileLog(CreateProfileLog(Settings, Timer, Scheduler))
    {
        ProfileLog->Start();
    }

    ~TEnv()
    {
        ProfileLog->Stop();
    }

    void ProcessLog(bool runScheduler = true)
    {
        if (runScheduler) {
            Scheduler->RunAllScheduledTasks();
        }

        EventProcessor.FlatMessages.clear();
        const char* argv[] = {"foo", Settings.FilePath.c_str()};
        IterateEventLog(NEvClass::Factory(), &EventProcessor, 2, argv);
        Sort(EventProcessor.FlatMessages);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProfileLogTest)
{
    Y_UNIT_TEST(TestDumpMessages)
    {
        TEnv env;
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(1),
             .Request = IProfileLog::TMiscRequest{
                 .RequestType = EBlockStoreRequest::MountVolume,
                 .Duration = TDuration::MilliSeconds(100),
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(2),
             .Request = IProfileLog::TReadWriteRequest{
                 .RequestType = EBlockStoreRequest::WriteBlocks,
                 .Duration = TDuration::MilliSeconds(200),
                 .PostponedTime = {},
                 .Range = TBlockRange64::WithLength(10, 20),
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(2),
             .Request = IProfileLog::TReadWriteRequestBlockInfos{
                 .RequestType = EBlockStoreRequest::WriteBlocks,
                 .BlockInfos =
                     {
                         {.BlockIndex=10, .Checksum=111},
                         {.BlockIndex=15, .Checksum=222},
                     },
                 .CommitId = 100500}});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(3),
             .Request = IProfileLog::TReadWriteRequest{
                 .RequestType = EBlockStoreRequest::WriteBlocks,
                 .Duration = TDuration::MilliSeconds(150),
                 .PostponedTime = {},
                 .Range = TBlockRange64::WithLength(50, 10),
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(4),
             .Request = IProfileLog::TReadWriteRequest{
                 .RequestType = EBlockStoreRequest::ReadBlocks,
                 .Duration = TDuration::MilliSeconds(500),
                 .PostponedTime = {},
                 .Range = TBlockRange64::WithLength(0, 100),
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(5),
             .Request = IProfileLog::TSysReadWriteRequest{
                 .RequestType = ESysRequestType::Flush,
                 .Duration = TDuration::MilliSeconds(300),
                 .Ranges =
                     {
                         TBlockRange64::WithLength(10, 10),
                         TBlockRange64::WithLength(30, 5),
                     },
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(5),
             .Request = IProfileLog::TSysReadWriteRequestBlockInfos{
                 .RequestType = ESysRequestType::Flush,
                 .BlockInfos =
                     {
                         {.BlockIndex = 11, .Checksum = 333},
                         {.BlockIndex = 15, .Checksum = 444},
                         {.BlockIndex = 33, .Checksum = 555},
                     },
                 .CommitId = 100501}});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(5) + TDuration::MilliSeconds(500),
             .Request = IProfileLog::TSysReadWriteRequest{
                 .RequestType = ESysRequestType::Compaction,
                 .Duration = TDuration::MilliSeconds(700),
                 .Ranges =
                     {
                         TBlockRange64::WithLength(0, 1024),
                     },
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(5) + TDuration::MilliSeconds(500),
             .Request = IProfileLog::TSysReadWriteRequestBlockInfos{
                 .RequestType = ESysRequestType::Compaction,
                 .BlockInfos =
                     {
                         {.BlockIndex = 11, .Checksum = 777},
                     },
                 .CommitId = 100502}});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(5) + TDuration::MilliSeconds(700),
             .Request = IProfileLog::TSysReadWriteRequest{
                 .RequestType = ESysRequestType::Cleanup,
                 .Duration = TDuration::MilliSeconds(300),
                 .Ranges =
                     {
                         TBlockRange64::WithLength(1024, 4096),
                     },
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk2",
             .Ts = TInstant::Seconds(2),
             .Request = IProfileLog::TReadWriteRequest{
                 .RequestType = EBlockStoreRequest::ZeroBlocks,
                 .Duration = TDuration::MilliSeconds(400),
                 .PostponedTime = {},
                 .Range = TBlockRange64::WithLength(5, 10),
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(6),
             .Request = IProfileLog::TMiscRequest{
                 .RequestType = EBlockStoreRequest::UnmountVolume,
                 .Duration = TDuration::MilliSeconds(50),
             }});

        env.ProcessLog();

        UNIT_ASSERT_VALUES_EQUAL(12, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:01.000000Z\tdisk1\t0\tMountVolume\tR\t100000\t0,0\n",
            env.EventProcessor.FlatMessages[0]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:02.000000Z\tdisk1\t0\tWriteBlocks\tB\t100500\t10:111 15:222\n",
            env.EventProcessor.FlatMessages[1]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:02.000000Z\tdisk1\t0\tWriteBlocks\tR\t200000\t10,20\n",
            env.EventProcessor.FlatMessages[2]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:02.000000Z\tdisk2\t0\tZeroBlocks\tR\t400000\t5,10\n",
            env.EventProcessor.FlatMessages[3]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:03.000000Z\tdisk1\t0\tWriteBlocks\tR\t150000\t50,10\n",
            env.EventProcessor.FlatMessages[4]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:04.000000Z\tdisk1\t0\tReadBlocks\tR\t500000\t0,100\n",
            env.EventProcessor.FlatMessages[5]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:05.000000Z\tdisk1\t0\tFlush\tB\t100501\t11:333 15:444 33:555\n",
            env.EventProcessor.FlatMessages[6]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:05.000000Z\tdisk1\t0\tFlush\tR\t300000\t10,10,30,5\n",
            env.EventProcessor.FlatMessages[7]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:05.500000Z\tdisk1\t0\tCompaction\tB\t100502\t11:777\n",
            env.EventProcessor.FlatMessages[8]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:05.500000Z\tdisk1\t0\tCompaction\tR\t700000\t0,1024\n",
            env.EventProcessor.FlatMessages[9]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:05.700000Z\tdisk1\t0\tCleanup\tR\t300000\t1024,4096\n",
            env.EventProcessor.FlatMessages[10]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:06.000000Z\tdisk1\t0\tUnmountVolume\tR\t50000\t0,0\n",
            env.EventProcessor.FlatMessages[11]
        );

        env.ProfileLog->Write(
            {.DiskId = "disk2",
             .Ts = TInstant::Seconds(8),
             .Request = IProfileLog::TMiscRequest{
                 .RequestType = EBlockStoreRequest::UnmountVolume,
                 .Duration = TDuration::MilliSeconds(500),
             }});
        env.ProfileLog->Write(
            {.DiskId = "disk2",
             .Ts = TInstant::Seconds(9),
             .Request = IProfileLog::TDescribeBlocksRequest{
                 .RequestType = EPrivateRequestType::DescribeBlocks,
                 .Duration = TDuration::MilliSeconds(70),
                 .Range = TBlockRange64::WithLength(17, 91),
             }});

        env.ProcessLog();

        UNIT_ASSERT_VALUES_EQUAL(14, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:08.000000Z\tdisk2\t0\tUnmountVolume\tR\t500000\t0,0\n",
            env.EventProcessor.FlatMessages[12]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:09.000000Z\tdisk2\t0\tDescribeBlocks\tR\t70000\t17,91\n",
            env.EventProcessor.FlatMessages[13]
        );
    }

    Y_UNIT_TEST(TestSmoke)
    {
        TEnv env;
        const EBlockStoreRequest requestTypes[] = {
            EBlockStoreRequest::ReadBlocks,
            EBlockStoreRequest::WriteBlocks,
            EBlockStoreRequest::ZeroBlocks,
        };
        for (ui32 i = 1; i <= 100000; ++i) {
            env.ProfileLog->Write(
                {.DiskId = TStringBuilder() << "disk" << (i % 10),
                 .Ts = TInstant::MilliSeconds(i),
                 .Request = IProfileLog::TReadWriteRequest{
                     .RequestType = requestTypes[i % 3],
                     .Duration = TDuration::MilliSeconds(i % 200),
                     .PostponedTime = {},
                     .Range = TBlockRange64::WithLength(i, i * 2),
                 }});
        }

        env.ProcessLog();
        UNIT_ASSERT_VALUES_EQUAL(100000, env.EventProcessor.FlatMessages.size());
    }

    Y_UNIT_TEST(TestStoresPostponedTime)
    {
        TEnv env;
        env.ProfileLog->Write(
            {.DiskId = "disk1",
             .Ts = TInstant::Seconds(2),
             .Request = IProfileLog::TReadWriteRequest{
                 .RequestType = EBlockStoreRequest::WriteBlocks,
                 .Duration = TDuration::MilliSeconds(200),
                 .PostponedTime = TDuration::MilliSeconds(42),
                 .Range = TBlockRange64::WithLength(10, 20),
             }});
        env.ProcessLog();
        UNIT_ASSERT_VALUES_EQUAL(1, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:02.000000Z\tdisk1\t0\tWriteBlocks\tR\t200000\t10,20\n",
            env.EventProcessor.FlatMessages[0]
        );
    }

    Y_UNIT_TEST(TestFlushOnDestruct)
    {
        TEnv env;
        env.ProfileLog->Write(
            {.DiskId = "disk2",
             .Ts = TInstant::Seconds(3),
             .Request = IProfileLog::TReadWriteRequest{
                 .RequestType = EBlockStoreRequest::WriteBlocks,
                 .Duration = TDuration::MilliSeconds(300),
                 .PostponedTime = TDuration::MilliSeconds(42),
                 .Range = TBlockRange64::WithLength(10, 20),
             }});
        env.ProfileLog = CreateProfileLogStub();
        env.ProcessLog(false);
        UNIT_ASSERT_VALUES_EQUAL(1, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:03.000000Z\tdisk2\t0\tWriteBlocks\tR\t300000\t10,20\n",
            env.EventProcessor.FlatMessages[0]);
    }

    Y_UNIT_TEST(TestReplicaChecksums)
    {
        TEnv env;
        env.ProfileLog->Write({
            .DiskId = "disk3",
            .Ts = TInstant::Seconds(3),
            .Request =
                IProfileLog::TSysReadWriteRequestWithChecksums{
                    .RequestType = ESysRequestType::ResyncChecksum,
                    .Duration = TDuration::MilliSeconds(100),
                    .RangeInfo =
                        {.Range = TBlockRange64::WithLength(10, 10),
                         .ReplicaChecksums = {IProfileLog::TReplicaChecksums{
                             .ReplicaId = 2,
                             .Checksums = {100}}}}},
        });

        env.ProfileLog->Write({
            .DiskId = "disk3",
            .Ts = TInstant::Seconds(4),
            .Request =
                IProfileLog::TSysReadWriteRequestWithChecksums{
                    .RequestType = ESysRequestType::ResyncRead,
                    .Duration = TDuration::MilliSeconds(100),
                    .RangeInfo =
                        {.Range = TBlockRange64::WithLength(10, 10),
                         .ReplicaChecksums = {IProfileLog::TReplicaChecksums{
                             .ReplicaId = 2,
                             .Checksums = {1000, 2000, 3000}}}}},
        });

        env.ProfileLog->Write({
            .DiskId = "disk3",
            .Ts = TInstant::Seconds(5),
            .Request =
                IProfileLog::TSysReadWriteRequestWithChecksums{
                    .RequestType = ESysRequestType::ResyncWrite,
                    .Duration = TDuration::MilliSeconds(100),
                    .RangeInfo =
                        {.Range = TBlockRange64::WithLength(10, 10),
                         .ReplicaChecksums = {IProfileLog::TReplicaChecksums{
                             .ReplicaId = 2,
                             .Checksums = {1000, 2000, 3000}}}}

                },
        });

        env.ProfileLog = CreateProfileLogStub();
        env.ProcessLog(false);
        UNIT_ASSERT_VALUES_EQUAL(3, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:03.000000Z\tdisk3\t0\tResyncChecksum\tR\t100000\t10,10{\"2\":[100]}\n",
            env.EventProcessor.FlatMessages[0]);
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:04.000000Z\tdisk3\t0\tResyncRead\tR\t100000\t10,10{\"2\":[1000,2000,3000]}\n",
            env.EventProcessor.FlatMessages[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            "1970-01-01T00:00:05.000000Z\tdisk3\t0\tResyncWrite\tR\t100000\t10,10{\"2\":[1000,2000,3000]}\n",
            env.EventProcessor.FlatMessages[2]);
    }
}

}   // namespace NCloud::NBlockStore
