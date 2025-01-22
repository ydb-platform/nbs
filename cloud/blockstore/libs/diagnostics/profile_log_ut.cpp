#include "profile_log.h"

#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
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

using TRanges =
    google::protobuf::RepeatedPtrField<NProto::TProfileLogBlockRange>;

TString RangesToString(const TRanges& ranges)
{
    TStringBuilder sb;
    for (int i = 0; i < ranges.size(); ++i) {
        if (i) {
            sb << " ";
        }

        sb << ranges[i].GetBlockIndex() << "," << ranges[i].GetBlockCount();
    }
    return sb;
}

using TBlockInfos =
    google::protobuf::RepeatedPtrField<NProto::TProfileLogBlockInfo>;

TString BlockInfosToString(const TBlockInfos& blockInfos)
{
    TStringBuilder sb;
    for (int i = 0; i < blockInfos.size(); ++i) {
        if (i) {
            sb << " ";
        }

        sb << blockInfos[i].GetBlockIndex() << ":" << blockInfos[i].GetChecksum();
    }
    return sb;
}

struct TEventProcessor
    : TProtobufEventProcessor
{
    TVector<TString> FlatMessages;

    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        Y_UNUSED(out);

        auto message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message) {
            for (const auto& r: message->GetRequests()) {
                FlatMessages.push_back(
                    TStringBuilder() << message->GetDiskId()
                        << "\t" << r.GetTimestampMcs()
                        << "\tR"
                        << "\t" << r.GetRequestType()
                        << "\t" << r.GetDurationMcs()
                        << "\t" << r.GetPostponedTimeMcs()
                        << "\t" << RangesToString(r.GetRanges())
                );
            }

            for (const auto& bl: message->GetBlockInfoLists()) {
                FlatMessages.push_back(
                    TStringBuilder() << message->GetDiskId()
                        << "\t" << bl.GetTimestampMcs()
                        << "\tB"
                        << "\t" << bl.GetRequestType()
                        << "\t" << bl.GetCommitId()
                        << "\t" << BlockInfosToString(bl.GetBlockInfos())
                );
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
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(1),
            IProfileLog::TMiscRequest{
                EBlockStoreRequest::MountVolume,
                TDuration::MilliSeconds(100),
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(2),
            IProfileLog::TReadWriteRequest{
                EBlockStoreRequest::WriteBlocks,
                TDuration::MilliSeconds(200),
                {},
                TBlockRange64::WithLength(10, 20),
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(2),
            IProfileLog::TReadWriteRequestBlockInfos{
                EBlockStoreRequest::WriteBlocks,
                {
                    {10, 111},
                    {15, 222},
                },
                100500
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(3),
            IProfileLog::TReadWriteRequest{
                EBlockStoreRequest::WriteBlocks,
                TDuration::MilliSeconds(150),
                {},
                TBlockRange64::WithLength(50, 10),
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(4),
            IProfileLog::TReadWriteRequest{
                EBlockStoreRequest::ReadBlocks,
                TDuration::MilliSeconds(500),
                {},
                TBlockRange64::WithLength(0, 100),
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(5),
            IProfileLog::TSysReadWriteRequest{
                ESysRequestType::Flush,
                TDuration::MilliSeconds(300),
                {
                    TBlockRange64::WithLength(10, 10),
                    TBlockRange64::WithLength(30, 5),
                },
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(5),
            IProfileLog::TSysReadWriteRequestBlockInfos{
                ESysRequestType::Flush,
                {
                    {11, 333},
                    {15, 444},
                    {33, 555},
                },
                100501
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(5) + TDuration::MilliSeconds(500),
            IProfileLog::TSysReadWriteRequest{
                ESysRequestType::Compaction,
                TDuration::MilliSeconds(700),
                {
                    TBlockRange64::WithLength(0, 1024),
                },
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(5) + TDuration::MilliSeconds(500),
            IProfileLog::TSysReadWriteRequestBlockInfos{
                ESysRequestType::Compaction,
                {
                    {11, 777},
                },
                100502
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(5) + TDuration::MilliSeconds(700),
            IProfileLog::TSysReadWriteRequest{
                ESysRequestType::Cleanup,
                TDuration::MilliSeconds(300),
                {
                    TBlockRange64::WithLength(1024, 4096),
                },
            }
        });
        env.ProfileLog->Write({
            "disk2",
            TInstant::Seconds(2),
            IProfileLog::TReadWriteRequest{
                EBlockStoreRequest::ZeroBlocks,
                TDuration::MilliSeconds(400),
                {},
                TBlockRange64::WithLength(5, 10),
            }
        });
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(6),
            IProfileLog::TMiscRequest{
                EBlockStoreRequest::UnmountVolume,
                TDuration::MilliSeconds(50),
            }
        });

        env.ProcessLog();

        UNIT_ASSERT_VALUES_EQUAL(12, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t1000000\tR\t6\t100000\t0\t",
            env.EventProcessor.FlatMessages[0]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t2000000\tB\t9\t100500\t10:111 15:222",
            env.EventProcessor.FlatMessages[1]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t2000000\tR\t9\t200000\t0\t10,20",
            env.EventProcessor.FlatMessages[2]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t3000000\tR\t9\t150000\t0\t50,10",
            env.EventProcessor.FlatMessages[3]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t4000000\tR\t8\t500000\t0\t0,100",
            env.EventProcessor.FlatMessages[4]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t5000000\tB\t10001\t100501\t11:333 15:444 33:555",
            env.EventProcessor.FlatMessages[5]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t5000000\tR\t10001\t300000\t0\t10,10 30,5",
            env.EventProcessor.FlatMessages[6]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t5500000\tB\t10000\t100502\t11:777",
            env.EventProcessor.FlatMessages[7]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t5500000\tR\t10000\t700000\t0\t0,1024",
            env.EventProcessor.FlatMessages[8]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t5700000\tR\t10004\t300000\t0\t1024,4096",
            env.EventProcessor.FlatMessages[9]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t6000000\tR\t7\t50000\t0\t",
            env.EventProcessor.FlatMessages[10]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk2\t2000000\tR\t10\t400000\t0\t5,10",
            env.EventProcessor.FlatMessages[11]
        );

        env.ProfileLog->Write({
            "disk2",
            TInstant::Seconds(8),
            IProfileLog::TMiscRequest{
                EBlockStoreRequest::UnmountVolume,
                TDuration::MilliSeconds(500),
            }
        });
        env.ProfileLog->Write({
            "disk2",
            TInstant::Seconds(9),
            IProfileLog::TDescribeBlocksRequest{
                EPrivateRequestType::DescribeBlocks,
                TDuration::MilliSeconds(70),
                TBlockRange64::WithLength(17, 91),
            }
        });

        env.ProcessLog();

        UNIT_ASSERT_VALUES_EQUAL(14, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "disk2\t8000000\tR\t7\t500000\t0\t",
            env.EventProcessor.FlatMessages[12]
        );
        UNIT_ASSERT_VALUES_EQUAL(
            "disk2\t9000000\tR\t20000\t70000\t0\t17,91",
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
            env.ProfileLog->Write({
                TStringBuilder() << "disk" << (i % 10),
                TInstant::MilliSeconds(i),
                IProfileLog::TReadWriteRequest{
                    requestTypes[i % 3],
                    TDuration::MilliSeconds(i % 200),
                    {},
                    TBlockRange64::WithLength(i, i * 2),
                }
            });
        }

        env.ProcessLog();
        UNIT_ASSERT_VALUES_EQUAL(100000, env.EventProcessor.FlatMessages.size());
    }

    Y_UNIT_TEST(TestStoresPostponedTime)
    {
        TEnv env;
        env.ProfileLog->Write({
            "disk1",
            TInstant::Seconds(2),
            IProfileLog::TReadWriteRequest{
                EBlockStoreRequest::WriteBlocks,
                TDuration::MilliSeconds(200),
                TDuration::MilliSeconds(42),
                TBlockRange64::WithLength(10, 20),
            }
        });
        env.ProcessLog();
        UNIT_ASSERT_VALUES_EQUAL(1, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "disk1\t2000000\tR\t9\t200000\t42000\t10,20",
            env.EventProcessor.FlatMessages[0]
        );
    }

    Y_UNIT_TEST(TestFlushOnDestruct)
    {
        TEnv env;
        env.ProfileLog->Write(
            {"disk2",
             TInstant::Seconds(3),
             IProfileLog::TReadWriteRequest{
                 EBlockStoreRequest::WriteBlocks,
                 TDuration::MilliSeconds(300),
                 TDuration::MilliSeconds(42),
                 TBlockRange64::WithLength(10, 20),
             }});
        env.ProfileLog = CreateProfileLogStub();
        env.ProcessLog(false);
        UNIT_ASSERT_VALUES_EQUAL(1, env.EventProcessor.FlatMessages.size());
        UNIT_ASSERT_VALUES_EQUAL(
            "disk2\t3000000\tR\t9\t300000\t42000\t10,20",
            env.EventProcessor.FlatMessages[0]);
    }
}

}   // namespace NCloud::NBlockStore
