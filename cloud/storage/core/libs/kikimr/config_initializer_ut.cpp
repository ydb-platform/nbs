#include "config_initializer.h"
#include "options.h"

#include <cloud/storage/core/libs/common/affinity.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/printf.h>

#include <iterator>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsYdb
    : public TOptionsYdbBase
{
    void Parse(int argc, char** argv) override
    {
        NLastGetopt::TOptsParseResultException(&Opts, argc, argv);
    }
};

TOptionsYdbBasePtr CreateOptions()
{
    return std::make_shared<TOptionsYdb>();
}

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerYdb
    : public TConfigInitializerYdbBase
{
    explicit TConfigInitializerYdb(TOptionsYdbBasePtr options)
        : TConfigInitializerYdbBase(std::move(options))
    {}

    void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig&) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigInitializerTest)
{
    Y_UNIT_TEST(ShouldLoadSysConfig)
    {
        auto sysConfigStr = R"(
            Executor {
                Type: BASIC
                Threads: 1
                Name: "System"
            }
            Executor {
                Type: BASIC
                Threads: 2
                Name: "User"
            }
            Executor {
                Type: BASIC
                Threads: 3
                Name: "Batch"
            }
            Executor {
                Type: IO
                Threads: 4
                Name: "IO"
            }
            Executor {
                Type: BASIC
                Threads: 5
                Name: "IC"
            }
        )";

        TTempDir dir;
        auto sysConfigPath = dir.Path() / "nbs-sys.txt";
        TOFStream(sysConfigPath.GetPath()).Write(sysConfigStr);

        auto options = CreateOptions();
        options->SysConfig = sysConfigPath.GetPath();

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitKikimrConfig();

        const auto& actualConfig = ci.KikimrConfig->GetActorSystemConfig();
        UNIT_ASSERT_VALUES_EQUAL(5, actualConfig.GetExecutor().size());
        UNIT_ASSERT_VALUES_EQUAL(1, actualConfig.GetExecutor(0).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(2, actualConfig.GetExecutor(1).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(3, actualConfig.GetExecutor(2).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(4, actualConfig.GetExecutor(3).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(5, actualConfig.GetExecutor(4).GetThreads());
    }

    void TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
        ui32 systemThreads,
        ui32 userThreads,
        ui32 batchThreads,
        ui32 ioThreads,
        ui32 icThreads,
        ui32 expectedSystemThreads,
        ui32 expectedUserThreads,
        ui32 expectedBatchThreads,
        ui32 expectedIoThreads,
        ui32 expectedIcThreads,
        ui32 totalCores,
        ui32 availableCoresPercentage)
    {
        auto sysConfigStr = Sprintf(
            R"(
                Executor {
                    Type: BASIC
                    Threads: %u
                    Name: "System"
                }
                Executor {
                    Type: BASIC
                    Threads: %u
                    Name: "User"
                }
                Executor {
                    Type: BASIC
                    Threads: %u
                    Name: "Batch"
                }
                Executor {
                    Type: IO
                    Threads: %u
                    Name: "IO"
                }
                Executor {
                    Type: BASIC
                    Threads: %u
                    Name: "IC"
                }
            )",
            systemThreads,
            userThreads,
            batchThreads,
            ioThreads,
            icThreads);

        TTempDir dir;
        auto sysConfigPath = dir.Path() / "nbs-sys.txt";
        TOFStream(sysConfigPath.GetPath()).Write(sysConfigStr);

        auto options = CreateOptions();
        options->SysConfig = sysConfigPath.GetPath();

        char programName[] = "./program";
        char flagName[] = "--actor-system-available-cpu-cores-percentage";
        TString availableCpuCoresPercentageStr =
            TStringBuilder() << availableCoresPercentage;
        char* argv[] = {
            programName,
            flagName,
            availableCpuCoresPercentageStr.begin(),
        };
        options->Parse(3, argv);
        UNIT_ASSERT_VALUES_EQUAL(
            availableCoresPercentage,
            options->ActorSystemAvailableCpuCoresPercentage);

        TVector<ui8> totalCoresVec;
        for (ui8 i = 0; i < totalCores; i++) {
            totalCoresVec.push_back(i);
        }
        TAffinity affinity(totalCoresVec);
        TAffinityGuard affinityGuard(affinity);

        auto ci = TConfigInitializerYdb(std::move(options));
        ci.InitKikimrConfig();

        const auto& actualConfig = ci.KikimrConfig->GetActorSystemConfig();
        UNIT_ASSERT_VALUES_EQUAL(5, actualConfig.GetExecutor().size());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedSystemThreads,
            actualConfig.GetExecutor(0).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedUserThreads,
            actualConfig.GetExecutor(1).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedBatchThreads,
            actualConfig.GetExecutor(2).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedIoThreads,
            actualConfig.GetExecutor(3).GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(
            expectedIcThreads,
            actualConfig.GetExecutor(4).GetThreads());
    }

    Y_UNIT_TEST(ShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores)
    {
        // should not affect configuration with available cores percentage set
        // to zero
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            12, 12, 1, 1, 12,  // System, User, Batch, IO, IC
            12, 12, 1, 1, 12,  // System, User, Batch, IO, IC
            36,                // availableCores
            0                  // availableCoresPercentage
        );
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            12, 12, 1, 1, 12,  // System, User, Batch, IO, IC
            12, 12, 1, 1, 12,  // System, User, Batch, IO, IC
            36,                // availableCores
            100                // availableCoresPercentage
        );
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            12, 12, 1, 1, 12,  // System, User, Batch, IO, IC
            6,   6, 1, 1,  6,  // System, User, Batch, IO, IC
            36,                // availableCores
            50                 // availableCoresPercentage
        );
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            10, 10, 1, 1, 10,  // System, User, Batch, IO, IC
            10, 10, 1, 1, 10,  // System, User, Batch, IO, IC
            58,                // availableCores
            50                 // availableCoresPercentage
        );
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            10, 10, 1, 1, 10,  // System, User, Batch, IO, IC
            7,   7, 1, 1,  7,  // System, User, Batch, IO, IC
            40,                // availableCores
            50                 // availableCoresPercentage
        );
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            10, 10, 1, 1, 10,  // System, User, Batch, IO, IC
            6,   6, 1, 1,  6,  // System, User, Batch, IO, IC
            32,                // availableCores
            50                 // availableCoresPercentage
        );
        // should not set thread count to zero
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            1, 1, 1, 1, 1,  // System, User, Batch, IO, IC
            1, 1, 1, 1, 1,  // System, User, Batch, IO, IC
            1,              // availableCores
            1               // availableCoresPercentage
        );
        // should not affect configuration with non equal-sized
        // System, User, IC thread pools
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            11, 12, 1, 1, 12,  // System, User, Batch, IO, IC
            11, 12, 1, 1, 12,  // System, User, Batch, IO, IC
            36,                // availableCores
            50                 // availableCoresPercentage
        );
        // should not affect configuration with non equal-sized
        // System, User, IC thread pools
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            12, 12, 1, 1, 11,  // System, User, Batch, IO, IC
            12, 12, 1, 1, 11,  // System, User, Batch, IO, IC
            36,                // availableCores
            50                 // availableCoresPercentage
        );
        // should not affect configuration with non equal-sized
        // System, User, IC thread pools
        TestShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores(
            12, 11, 1, 1, 11,  // System, User, Batch, IO, IC
            12, 11, 1, 1, 11,  // System, User, Batch, IO, IC
            36,                // availableCores
            50                 // availableCoresPercentage
        );
    }
}

}   // namespace NCloud::NStorage
