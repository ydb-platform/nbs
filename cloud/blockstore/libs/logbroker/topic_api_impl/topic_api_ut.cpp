#include "topic_api.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>

#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>
#include <util/string/printf.h>
#include <util/system/hostname.h>

#include <chrono>

namespace NCloud::NBlockStore::NLogbroker {

using namespace NThreading;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString TestConsumer = "test-consumer";
const TString TestTopic = "test-topic";
const TString TestSource = "test-source";
const TString Database = "/Root";
const TDuration WaitTimeout = 15s;

////////////////////////////////////////////////////////////////////////////////

NKikimr::Tests::TServerSettings MakeServerSettings()
{
    using namespace NKikimrServices;
    using namespace NActors::NLog;

    auto settings = NKikimr::NPersQueueTests::PQSettings(0);
    settings.SetDomainName("Root");
    settings.SetNodeCount(1);
    settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
    settings.PQConfig.SetRoot("/Root");
    settings.PQConfig.SetDatabase("/Root");

    settings.SetLoggerInitializer([] (NActors::TTestActorRuntime& runtime) {
        runtime.SetLogPriority(PQ_READ_PROXY, PRI_DEBUG);
        runtime.SetLogPriority(PQ_WRITE_PROXY, PRI_DEBUG);
        runtime.SetLogPriority(PQ_MIRRORER, PRI_DEBUG);
        runtime.SetLogPriority(PQ_METACACHE, PRI_DEBUG);
        runtime.SetLogPriority(PERSQUEUE, PRI_DEBUG);
        runtime.SetLogPriority(PERSQUEUE_CLUSTER_TRACKER, PRI_DEBUG);
    });

    return settings;
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    std::optional<NPersQueue::TTestServer> Server;

    std::optional<NYdb::TDriver> Driver;
    std::optional<NYdb::NTopic::TTopicClient> Client;

    ILoggingServicePtr Logging = CreateLoggingService(
        "console",
        {
            .FiltrationLevel = TLOG_DEBUG,
        });

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Server.emplace(MakeServerSettings());

        Driver.emplace(NYdb::TDriverConfig()
            .SetEndpoint("localhost:" + ToString(Server->GrpcPort))
            .SetDatabase(Database));
        Client.emplace(*Driver);

        auto settings = NYdb::NTopic::TCreateTopicSettings()
            .PartitioningSettings(1, 1);

        NYdb::NTopic::TConsumerSettings consumers(
            settings,
            TestConsumer);

        settings.AppendConsumers(consumers);

        auto status = Client->CreateTopic(TestTopic, settings)
            .GetValueSync();

        UNIT_ASSERT_C(status.IsSuccess(), status);

        Server->WaitInit(TestTopic);
    }

    auto Read(size_t count)
    {
        using namespace NYdb::NTopic;

        auto session = Client->CreateReadSession(NYdb::NTopic::TReadSessionSettings()
            .ConsumerName(TestConsumer)
            .AppendTopics(TestTopic)
            .Decompress(true)
            .Log(Logging->CreateLog("Read")));

        TVector<TMessage> messages;

        bool sessionClosed = false;

        while (!sessionClosed) {
            session->WaitEvent().Wait();

            auto events = session->GetEvents();
            for (auto& event: events) {
                std::visit(TOverloaded {
                    [&] (TReadSessionEvent::TDataReceivedEvent& ev) {
                        UNIT_ASSERT(!ev.HasCompressedMessages());

                        for (auto& m: ev.GetMessages()) {
                            messages.emplace_back(TMessage{
                                m.GetData(),
                                m.GetSeqNo()
                            });
                        }

                        count -= std::min(ev.GetMessagesCount(), count);

                        ev.Commit();

                        if (!count) {
                            session->Close(1s);
                        }
                    },
                    [&] (TReadSessionEvent::TStartPartitionSessionEvent& ev) {
                        ev.Confirm();
                    },
                    [&] (TReadSessionEvent::TStopPartitionSessionEvent& ev) {
                        ev.Confirm();
                    },
                    [&] (TSessionClosedEvent& ev) {
                        UNIT_ASSERT_C(ev.IsSuccess(), ev.DebugString());
                        sessionClosed = true;
                    },
                    [] (const auto&) {}
                }, event);
            }
        }

        return messages;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogbrokerTest)
{
    Y_UNIT_TEST_F(ShouldWriteData, TFixture)
    {
        NProto::TLogbrokerConfig config;

        config.SetAddress("localhost");
        config.SetPort(Server->GrpcPort);
        config.SetDatabase(Database);
        config.SetTopic(TestTopic);
        config.SetSourceId(TestSource);

        auto service = CreateTopicAPIService(
            std::make_shared<TLogbrokerConfig>(config),
            Logging);

        service->Start();

        const TVector<TMessage> expectedData{
            {"hello", 42},
            {"world", 888},
            {"foo", 1000},
            {"bar", 1001},
        };

        {
            auto future =
                service->Write({expectedData[0], expectedData[1]}, Now());

            UNIT_ASSERT(future.Wait(WaitTimeout));

            const auto& error = future.GetValue();

            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        {
            auto future =
                service->Write({expectedData[2], expectedData[3]}, Now());

            UNIT_ASSERT(future.Wait(WaitTimeout));

            const auto& error = future.GetValue();

            UNIT_ASSERT_C(!HasError(error), FormatError(error));
        }

        auto data = Read(expectedData.size());

        UNIT_ASSERT_VALUES_EQUAL(expectedData.size(), data.size());
        for (size_t i = 0; i != expectedData.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(expectedData[i].Payload, data[i].Payload);
            UNIT_ASSERT_VALUES_EQUAL(expectedData[i].SeqNo, data[i].SeqNo);
        }

        service->Stop();
    }

    void ShouldHandleErrorImpl(TLogbrokerConfigPtr config)
    {
        auto logging = CreateLoggingService("console", TLogSettings{});

        auto service = CreateTopicAPIService(config, logging);
        service->Start();

        auto future = service->Write({TMessage{"hello", 42}}, Now());

        UNIT_ASSERT(future.Wait(WaitTimeout));

        const auto& error = future.GetValue();
        UNIT_ASSERT(HasError(error));

        service->Stop();
    }

    Y_UNIT_TEST_F(ShouldHandleConnectError, TFixture)
    {
        NProto::TLogbrokerConfig proto;

        proto.SetDatabase(Database);
        proto.SetTopic(TestTopic);
        proto.SetSourceId("test");
        proto.SetAddress("unknown");

        ShouldHandleErrorImpl(std::make_shared<TLogbrokerConfig>(proto));
    }

    Y_UNIT_TEST_F(ShouldHandleUnknownTopic, TFixture)
    {
        NProto::TLogbrokerConfig proto;

        proto.SetAddress("localhost");
        proto.SetPort(Server->GrpcPort);
        proto.SetDatabase(Database);
        proto.SetTopic("unknown-topic");
        proto.SetSourceId(Sprintf(
            "test:%s:%lu",
            GetFQDNHostName(),
            TInstant::Now().MilliSeconds()));

        ShouldHandleErrorImpl(std::make_shared<TLogbrokerConfig>(proto));
    }
}

}   // namespace NCloud::NBlockStore::NLogbroker
