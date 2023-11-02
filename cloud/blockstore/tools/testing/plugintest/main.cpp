#include "options.h"
#include "test.h"

#include <cloud/blockstore/tools/testing/plugintest/test.pb.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/yexception.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void RunTest(const TOptions& options)
{
    NProto::NTest::TPluginTest testConfig;
    ParseFromTextFormat(options.TestConfig, testConfig);

    TPluginTest test(
        options.PluginLibPath,
        options.PluginOptions,
        options.HostMajor,
        options.HostMinor,
        options.EndpointFolder);
    test.Start();
    test.GetVersion();

    BlockPlugin_Volume volume = {};
    for (const auto& r: testConfig.GetRequests()) {
        if (r.HasMountVolumeRequest()) {
            test.MountVolume(&volume, r.GetMountVolumeRequest());
        } else if (r.HasUnmountVolumeRequest()) {
            test.UnmountVolume(&volume, r.GetUnmountVolumeRequest());
        } else if (r.HasReadBlocksRequest()) {
            test.ReadBlocks(&volume, r.GetReadBlocksRequest());
        } else if (r.HasWriteBlocksRequest()) {
            test.WriteBlocks(&volume, r.GetWriteBlocksRequest());
        } else if (r.HasZeroBlocksRequest()) {
            test.ZeroBlocks(&volume, r.GetZeroBlocksRequest());
        }
    }

    if (volume.state) {
        // just in case
        test.UnmountVolume(&volume, {});
    }

    test.Stop();
}

}   // namespace NCloud::NBlockStore

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NBlockStore;

    auto options = std::make_shared<TOptions>();
    try {
        options->Parse(argc, argv);
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    try {
        for (ui32 i = 0; i < options->RunCount; ++i) {
            RunTest(*options);
        }
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }

    return 0;
}
