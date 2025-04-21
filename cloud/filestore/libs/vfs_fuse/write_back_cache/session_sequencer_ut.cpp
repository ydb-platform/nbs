#include "session_sequencer.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/random/random.h>

#include <thread>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    ILoggingServicePtr Logging;
    TLog Log;

    std::shared_ptr<TFileStoreTest> Session;
    TSessionSequencerPtr SessionSequencer;

    TCallContextPtr CallContext;

    TBootstrap()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("SESSION_SEQUENCER");

        Session = std::make_shared<TFileStoreTest>();

        Session->ReadDataHandler = [&] (auto, auto) {
            NProto::TReadDataResponse response;
            return MakeFuture(response);
        };

        Session->WriteDataHandler = [&] (auto, auto) {
            NProto::TWriteDataResponse response;
            return MakeFuture(response);
        };

        CallContext = MakeIntrusive<TCallContext>();
    }

    ~TBootstrap() = default;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSessionSequencerTest)
{
    Y_UNIT_TEST(ShouldReadAndWrite)
    {
        TBootstrap b;
    }
}

}   // namespace NCloud::NFileStore::NFuse
