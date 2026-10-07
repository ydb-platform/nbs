#include "options.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>
#include <util/system/env.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOptionsTest)
{
    Y_UNIT_TEST(ShouldReadLatencySliEnvironmentAndAllowExplicitOverride)
    {
        const auto previous = GetEnv("NBS_LATENCY_SLI_CONFIG");
        Y_SCOPE_EXIT(previous) {
            SetEnv("NBS_LATENCY_SLI_CONFIG", previous);
        };
        SetEnv("NBS_LATENCY_SLI_CONFIG", "1;0:1:8193:1000");
        TVector<TString> params{
            "binary-path", "--socket-path", "vhost.sock", "--serial", "id",
            "--device", "disk:1048576:0"};
        auto parse = [&] {
            TVector<char*> argv;
            for (auto& value: params) {
                argv.push_back(value.Detach());
            }
            TOptions options;
            options.Parse(argv.size(), argv.data());
            return options;
        };
        UNIT_ASSERT_VALUES_EQUAL("1;0:1:8193:1000", parse().LatencySli.Serialize());
        params.push_back("--latency-sli");
        params.push_back("1;1:1:8193:2000");
        UNIT_ASSERT_VALUES_EQUAL("1;1:1:8193:2000", parse().LatencySli.Serialize());
    }

    Y_UNIT_TEST(ShouldParseOptions)
    {
        TOptions options;

        TVector<TString> params {
            "binary-path",
            "--socket-path", "vhost.sock",
            "--disk-id", "disk-id",
            "--serial", "id",
            "--device", "path-nvme:v3-1:1000000:0",
            "--device", "path-nvme:v3-2:2000042:1111111",
            "--device", "path-nvme:v3-3:3001000:0",
            "--vmpte-flush-threshold", "12345678900",
            "--read-only"
        };

        TVector<char*> argv;
        for (auto& p: params) {
            argv.push_back(&p[0]);
        }

        options.Parse(argv.size(), argv.data());

        UNIT_ASSERT_VALUES_EQUAL("vhost.sock", options.SocketPath);
        UNIT_ASSERT_VALUES_EQUAL("id", options.Serial);
        UNIT_ASSERT_VALUES_EQUAL("disk-id", options.DiskId);
        UNIT_ASSERT(options.ReadOnly);
        UNIT_ASSERT(!options.NoSync);
        UNIT_ASSERT(!options.NoChmod);
        UNIT_ASSERT_VALUES_EQUAL(1024, options.BatchSize);
        UNIT_ASSERT_VALUES_EQUAL(12345678900, options.PteFlushByteThreshold);
        UNIT_ASSERT_VALUES_EQUAL(3, options.QueueCount);
        UNIT_ASSERT_VALUES_EQUAL(3, options.Layout.size());
        UNIT_ASSERT_VALUES_EQUAL("path-nvme:v3-1", options.Layout[0].DevicePath);
        UNIT_ASSERT_VALUES_EQUAL("path-nvme:v3-2", options.Layout[1].DevicePath);
        UNIT_ASSERT_VALUES_EQUAL("path-nvme:v3-3", options.Layout[2].DevicePath);

        UNIT_ASSERT_VALUES_EQUAL(1000000, options.Layout[0].ByteCount);
        UNIT_ASSERT_VALUES_EQUAL(2000042, options.Layout[1].ByteCount);
        UNIT_ASSERT_VALUES_EQUAL(3001000, options.Layout[2].ByteCount);

        UNIT_ASSERT_VALUES_EQUAL(0, options.Layout[0].Offset);
        UNIT_ASSERT_VALUES_EQUAL(1111111, options.Layout[1].Offset);
        UNIT_ASSERT_VALUES_EQUAL(0, options.Layout[2].Offset);
    }
}

}   // namespace NCloud::NBlockStore::NVHostServer
