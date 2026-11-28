#include <cloud/vm/api/blockstore-plugin.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/dynlib.h>
#include <util/system/tempfile.h>

#include <memory>

namespace {

using TPluginPtr = std::unique_ptr<BlockPlugin, BlockPlugin_PutPlugin_t>;

class TPluginApi
{
private:
    TDynamicLibrary Library;
    BlockPluginHost Host = {};
    BlockPlugin_GetPlugin_t GetPlugin = nullptr;
    BlockPlugin_PutPlugin_t PutPlugin = nullptr;

    TTempFileHandle ClientConfig;
    TTempFileHandle InvalidConfig;
    TString Options;

public:
    TPluginApi()
    {
#if defined(_darwin_)
        const auto path =
            BinaryPath("cloud/vm/blockstore/libblockstore-plugin.dylib");
#else
        const auto path =
            BinaryPath("cloud/vm/blockstore/libblockstore-plugin.so");
#endif

        Library.Open(path.c_str());

        // The plugin owns process-global gRPC and tracing state.
        Library.SetUnloadable(false);

        GetPlugin = reinterpret_cast<BlockPlugin_GetPlugin_t>(
            Library.Sym(BLOCK_PLUGIN_GET_PLUGIN_SYMBOL_NAME));

        PutPlugin = reinterpret_cast<BlockPlugin_PutPlugin_t>(
            Library.Sym(BLOCK_PLUGIN_PUT_PLUGIN_SYMBOL_NAME));

        Host.magic = BLOCK_PLUGIN_HOST_MAGIC;
        Host.version_major = BLOCK_PLUGIN_API_VERSION_MAJOR;
        Host.version_minor = BLOCK_PLUGIN_API_VERSION_MINOR;
        Host.instance_id = "nbs-7893-test";

        Host.complete_request = [](
            BlockPluginHost*,
            BlockPlugin_Completion*)
        {
            return 0;
        };

        Host.log_message = [](BlockPluginHost*, const char*) {};

        {
            TFileOutput out(ClientConfig.Name());
            out << R"pb(
                ClientConfig {
                    Host: "127.0.0.1"
                    Port: 1
                    InsecurePort: 0
                    ThreadsCount: 1
                    IpcType: IPC_GRPC
                    ClientId: "nbs-7893-test"
                    RequestTimeout: 100
                }
            )pb";
            out.Finish();
        }

        {
            TFileOutput out(InvalidConfig.Name());
            out << "{";
            out.Finish();
        }

        Options = TStringBuilder()
            << "ClientConfig: " << ClientConfig.Name().Quote();
    }

    TPluginPtr GetValidPlugin()
    {
        return TPluginPtr(
            GetPlugin(&Host, Options.c_str()),
            PutPlugin);
    }

    TPluginPtr GetInvalidPlugin()
    {
        return TPluginPtr(
            GetPlugin(&Host, InvalidConfig.Name().c_str()),
            PutPlugin);
    }
};

void AssertUsable(BlockPlugin* plugin)
{
    UNIT_ASSERT_C(
        plugin,
        "GetPlugin returned nullptr");

    UNIT_ASSERT_C(
        plugin->state,
        "GetPlugin returned an uninitialized plugin");

    UNIT_ASSERT(plugin->get_dynamic_counters);

    TString counters;
    const int result = plugin->get_dynamic_counters(
        plugin,
        [](const char* value, void* opaque) {
            *static_cast<TString*>(opaque) = value;
        },
        &counters);

    UNIT_ASSERT_VALUES_EQUAL(result, static_cast<int>(BLOCK_PLUGIN_E_OK));
    UNIT_ASSERT_C(
        !counters.empty(),
        "Counters callback was not populated");
}

}   // namespace

Y_UNIT_TEST_SUITE(TBlockPluginLifecycleTest)
{
    Y_UNIT_TEST(ShouldRetryAfterInitFailure)
    {
        TPluginApi api;

        auto failed = api.GetInvalidPlugin();
        UNIT_ASSERT(!failed);

        auto plugin = api.GetValidPlugin();
        AssertUsable(plugin.get());
    }

    Y_UNIT_TEST(ShouldAllowRepeatedInitFailures)
    {
        TPluginApi api;

        for (unsigned attempt = 0; attempt < 3; ++attempt) {
            auto failed = api.GetInvalidPlugin();
            UNIT_ASSERT_C(
                !failed,
                "Invalid config unexpectedly succeeded");
        }

        auto plugin = api.GetValidPlugin();
        AssertUsable(plugin.get());
    }

    Y_UNIT_TEST(ShouldReleaseOnlyAfterLastPut)
    {
        TPluginApi api;

        auto first = api.GetValidPlugin();
        AssertUsable(first.get());

        auto second = api.GetValidPlugin();
        AssertUsable(second.get());

        UNIT_ASSERT(first.get() == second.get());

        first.reset();
        AssertUsable(second.get());

        second.reset();

        // A fresh Get must parse options again after the last Put.
        auto failed = api.GetInvalidPlugin();
        UNIT_ASSERT_C(
            !failed,
            "Last Put did not release the plugin");

        auto restarted = api.GetValidPlugin();
        AssertUsable(restarted.get());
    }
}
