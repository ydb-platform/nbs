#include "silk_env.h"

#include <silk/fibers/fiber.h>
#include <silk/util/crash-dumper.h>
#include <silk/util/init.h>

#include <library/cpp/testing/common/env.h>

#include <util/generic/string.h>
#include <util/stream/output.h>
#include <util/system/env.h>
#include <util/system/fs.h>

#include <gtest/gtest.h>

namespace NCloud::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

//
// Point the crash dumper at the gdb scripts in the source tree. The
// dumper's own resolution ("next to the binary" via /proc/self/exe)
// does not work under ya: ya runs the test binary from its build root,
// where the scripts are absent. Respects a value already present in the
// environment.
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

//
// The crash dumper execs "gdb" through a PATH lookup. CRASH_DUMPER_GDB
// names a gdb executable (e.g. the output of "ya tool gdb --print-path")
// to use instead of the one on PATH, if any: its directory is prepended
// to PATH. The dumper process inherits the environment when it is forked
// in installCrashDumper, so this must run before it.
//

void SetUpCrashDumperGdb()
{
    const TString gdb = GetEnv("CRASH_DUMPER_GDB");
    if (!gdb) {
        return;
    }

    const auto slash = gdb.rfind('/');
    if (slash == TString::npos || gdb.substr(slash + 1) != "gdb") {
        Cerr << "CRASH_DUMPER_GDB must be a path to an executable named"
             << " gdb, ignoring: " << gdb << Endl;
        return;
    }

    const TString path = GetEnv("PATH");
    SetEnv(
        "PATH",
        path ? gdb.substr(0, slash) + ":" + path : gdb.substr(0, slash));
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
        // fork produces a clean single-threaded child.
        //
        // gtest sets global environments up once per run, except with
        // --gtest_repeat=-1 or --gtest_recreate_environments_when_repeating;
        // a second install would fork a second dumper.
        //

        static bool crashDumperInstalled = false;
        if (!crashDumperInstalled) {
            SetUpCrashDumperScriptDir();
            SetUpCrashDumperGdb();
            silk::installCrashDumper();
            crashDumperInstalled = true;
        }

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
