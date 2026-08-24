#include "command.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/system/tempfile.h>

#include <array>
#include <string>
#include <vector>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestCommand final
    : public TCommand
{
public:
    ui64 ObservedRequestTimeout = 0;

private:
    void Start() override
    {}

    void Stop() override
    {}

    bool Execute() override
    {
        ObservedRequestTimeout = ClientConfig->GetRequestTimeout().MilliSeconds();
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommandTest)
{
    Y_UNIT_TEST(ShouldOverrideRequestTimeoutFromConfig)
    {
        TTempFileHandle configFile;
        TOFStream(configFile.GetName()).Write(
            "ClientConfig { RequestTimeout: 30000 }");

        std::array<std::string, 5> args = {
            "filestore-client",
            "--config",
            configFile.GetName(),
            "--request-timeout",
            "190000"};

        std::vector<char*> argv;
        argv.reserve(args.size());
        for (auto& arg: args) {
            argv.push_back(arg.data());
        }

        TTestCommand command;
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            command.Run(argv.size(), argv.data()));
        UNIT_ASSERT_VALUES_EQUAL(190000, command.ObservedRequestTimeout);
    }
}

}   // namespace NCloud::NFileStore::NClient
