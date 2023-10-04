#include "config_initializer.h"
#include "options.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/system/sanitizers.h>

namespace NCloud::NFileStore::NDaemon {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ParseProtoTextFromString(const TString& text, T& dst)
{
    TStringInput in(text);
    ParseFromTextFormat(in, dst);
}

TOptionsServerPtr CreateOptions()
{
    auto options = std::make_shared<TOptionsServer>();
    return options;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigInitializerTest)
{
    Y_UNIT_TEST(ShouldIgnoreUnknownFieldsInLogConfigAndNfsConfigs)
    {
        auto logConfigStr = R"(
            Entry {
                Component: "FILESTORE_SERVER"
                Level: 6
            }
            Entry {
                Component: "UNKNOWN_COMPONENT"
                Level: 6
            }
            SysLog: true
            DefaultLevel: 4
            UnknownField: "xxx"
            SysLogService: "NFS_SERVER"
        )";

        auto nfsComponentConfigStr = R"(
            NoSuchField: "x"
        )";

        TTempDir dir;
        auto logConfigPath = dir.Path() / "nfs-log.txt";
        auto nbsComponentConfigPath = dir.Path() / "nfs-component.txt";

        TOFStream(logConfigPath.GetPath()).Write(logConfigStr);
        TOFStream(nbsComponentConfigPath.GetPath()).Write(nfsComponentConfigStr);

        auto options = CreateOptions();
        options->LogConfig = logConfigPath.GetPath();
        options->DiagnosticsConfig = nbsComponentConfigPath.GetPath();
        options->StorageConfig = nbsComponentConfigPath.GetPath();
        options->ServerConfig = nbsComponentConfigPath.GetPath();

        auto ci = TConfigInitializerServer(std::move(options));
        ci.InitKikimrConfig();
        ci.InitDiagnosticsConfig();
        ci.InitStorageConfig();

        const auto& logConfig = ci.KikimrConfig->GetLogConfig();
        UNIT_ASSERT(logConfig.GetSysLog());
        UNIT_ASSERT(logConfig.GetIgnoreUnknownComponents());
        UNIT_ASSERT_VALUES_EQUAL(4, logConfig.GetDefaultLevel());
        UNIT_ASSERT_VALUES_EQUAL("NFS_SERVER", logConfig.GetSysLogService());
        UNIT_ASSERT_VALUES_EQUAL(2, logConfig.EntrySize());
        UNIT_ASSERT_VALUES_EQUAL(
            "FILESTORE_SERVER",
            logConfig.GetEntry(0).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(0).GetLevel());
        UNIT_ASSERT_VALUES_EQUAL(
            "UNKNOWN_COMPONENT",
            logConfig.GetEntry(1).GetComponent());
        UNIT_ASSERT_VALUES_EQUAL(6, logConfig.GetEntry(1).GetLevel());
    }
}

}   // namespace NCloud::NFileStore::NDaemon
