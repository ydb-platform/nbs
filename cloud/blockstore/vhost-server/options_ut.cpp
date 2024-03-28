#include "options.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TOptionsTest)
{
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

    Y_UNIT_TEST(ShouldParseRdmaBackendOptions)
    {
        TOptions options;

        TVector<TString> params {
            "binary-path",
            "--serial", "id",
            "--socket-path", "vhost.sock",
            "-q" , "8",
            "--disk-id", "disk-id",
            "--device-backend", "rdma",
            "--block-size", "4096",
            "--device", "rdma://host1:port1/uuid1:1111111:0",
            "--device", "rdma://host2:port2/uuid2:2222222:0",
            "--read-only",
            "--perf-profile", "1:2:3:4"
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
        UNIT_ASSERT_VALUES_EQUAL(8, options.QueueCount);
        UNIT_ASSERT_VALUES_EQUAL(2, options.Layout.size());
        UNIT_ASSERT_VALUES_EQUAL("rdma://host1:port1/uuid1", options.Layout[0].DevicePath);
        UNIT_ASSERT_VALUES_EQUAL("rdma://host2:port2/uuid2", options.Layout[1].DevicePath);

        UNIT_ASSERT_VALUES_EQUAL(1111111, options.Layout[0].ByteCount);
        UNIT_ASSERT_VALUES_EQUAL(2222222, options.Layout[1].ByteCount);

        UNIT_ASSERT_VALUES_EQUAL(0, options.Layout[0].Offset);
        UNIT_ASSERT_VALUES_EQUAL(0, options.Layout[1].Offset);

        UNIT_ASSERT_VALUES_EQUAL(1, options.MaxReadBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(2, options.MaxReadIops);
        UNIT_ASSERT_VALUES_EQUAL(3, options.MaxWriteBandwidth);
        UNIT_ASSERT_VALUES_EQUAL(4, options.MaxWriteIops);
    }

}

}   // namespace NCloud::NBlockStore::NVHostServer
