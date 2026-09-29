#include "silk_env.h"

#include <silk/fibers/fiber.h>
#include <silk/util/crash-dumper.h>
#include <silk/util/init.h>

#include <library/cpp/testing/common/env.h>

#include <util/generic/string.h>
#include <util/system/env.h>
#include <util/system/fs.h>

#include <gtest/gtest.h>

namespace NCloud::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

//
// Point the crash dumper at the gdb scripts in the source tree. The
// dumper's own resolution ("next to the binary" via /proc/self/exe)
// does not work under ya: the binary path resolves through the
// build-cache symlink store where the scripts are absent. Respects a
// value already present in the environment.
//

void SetUpCrashDumperScriptDir()
{
    if (GetEnv("SILK_CRASH_DUMPER_SCRIPT_DIR")) {
        return;
    }

    const TString scriptDir =
        ArcadiaSourceRoot() + "/contrib/libs/silk/src/gdb";
    if (NFs::Exists(scriptDir + "/crash-dumper.py")) {
        SetEnv("SILK_CRASH_DUMPER_SCRIPT_DIR", scriptDir);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSilkEnv: public ::testing::Environment
{
public:
    void SetUp() override
    {
        //
        // The dumper must be installed before the scheduler spawns its
        // threads: it forks the dumper process here, and a pre-thread
        // fork produces a clean single-threaded child. Repeated SetUp
        // calls (gtest_repeat) are safe - install is idempotent.
        //

        SetUpCrashDumperScriptDir();
        silk::installCrashDumper();

        silk::initialize();
        silk::FiberScheduler::initialize();
    }

    void TearDown() override
    {
        silk::FiberScheduler::destroy();
        silk::destroy();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

::testing::Environment* MakeSilkTestEnv()
{
    return new TSilkEnv;
}

}   // namespace NCloud::NFastShard
