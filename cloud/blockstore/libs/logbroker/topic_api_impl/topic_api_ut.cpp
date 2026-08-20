#include "topic_api.h"

#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/vector.h>

#include <utility>

namespace NCloud::NBlockStore::NLogbroker {

using namespace NThreading;
using namespace NYdb::NTopic;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TContinuationTokenIssuer final
    : private NYdb::NTopic::TContinuationTokenIssuer
{
public:
    static TContinuationToken Issue()
    {
        return IssueContinuationToken();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestWriteSession final
    : public IWriteSession
{
private:
    std::vector<TWriteSessionEvent::TEvent> Events;
    TVector<TMessage> Messages;

public:
    explicit TTestWriteSession(
        std::optional<NYdb::EStatus> initialError = std::nullopt)
    {
        if (initialError) {
            Events.emplace_back(TSessionClosedEvent(
                *initialError,
                NYdb::NIssue::TIssues{}));
        } else {
            AddReadyToAcceptEvent();
        }
    }

    const TVector<TMessage>& GetMessages() const
    {
        return Messages;
    }

    TFuture<void> WaitEvent() override
    {
        UNIT_ASSERT(!Events.empty());
        return MakeFuture();
    }

    std::optional<TWriteSessionEvent::TEvent> GetEvent(bool) override
    {
        if (Events.empty()) {
            return std::nullopt;
        }

        auto event = std::move(Events.front());
        Events.erase(Events.begin());
        return event;
    }

    std::vector<TWriteSessionEvent::TEvent> GetEvents(
        bool,
        std::optional<size_t>) override
    {
        return std::exchange(Events, {});
    }

    TFuture<uint64_t> GetInitSeqNo() override
    {
        return MakeFuture<uint64_t>(0);
    }

    void Write(
        TContinuationToken&&,
        TWriteMessage&&,
        TTransaction*) override
    {
        UNIT_FAIL("unexpected TWriteMessage overload");
    }

    void Write(
        TContinuationToken&&,
        std::string_view data,
        std::optional<uint64_t> seqNo,
        std::optional<TInstant>) override
    {
        UNIT_ASSERT(seqNo.has_value());
        Messages.push_back({TString{data}, *seqNo});

        TWriteSessionEvent::TAcksEvent event;
        event.Acks.push_back({
            .SeqNo = *seqNo,
            .State = TWriteSessionEvent::TWriteAck::EES_WRITTEN,
        });
        Events.emplace_back(std::move(event));
        AddReadyToAcceptEvent();
    }

    void WriteEncoded(
        TContinuationToken&&,
        TWriteMessage&&,
        TTransaction*) override
    {
        UNIT_FAIL("unexpected WriteEncoded overload");
    }

    void WriteEncoded(
        TContinuationToken&&,
        std::string_view,
        ECodec,
        uint32_t,
        std::optional<uint64_t>,
        std::optional<TInstant>) override
    {
        UNIT_FAIL("unexpected WriteEncoded overload");
    }

    bool Close(TDuration) override
    {
        return true;
    }

    TWriterCounters::TPtr GetCounters() override
    {
        return {};
    }

private:
    void AddReadyToAcceptEvent()
    {
        Events.emplace_back(TWriteSessionEvent::TReadyToAcceptEvent(
            TContinuationTokenIssuer::Issue()));
    }
};

////////////////////////////////////////////////////////////////////////////////

TLogbrokerConfigPtr CreateConfig()
{
    NProto::TLogbrokerConfig config;
    config.SetTopic("test-topic");
    config.SetSourceId("test-source");
    return std::make_shared<TLogbrokerConfig>(std::move(config));
}

IServicePtr CreateTestService(std::shared_ptr<TTestWriteSession> session)
{
    auto logging = CreateLoggingService("console", TLogSettings{});

    return CreateTopicAPIService(
        CreateConfig(),
        std::move(logging),
        {},
        [session = std::move(session)]
        {
            return session;
        });
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogbrokerTest)
{
    Y_UNIT_TEST(ShouldWriteData)
    {
        auto session = std::make_shared<TTestWriteSession>();
        auto service = CreateTestService(session);
        service->Start();

        const TVector<TMessage> expectedData{
            {"hello", 42},
            {"world", 888},
            {"foo", 1000},
            {"bar", 1001},
        };

        auto first = service->Write(
            {expectedData[0], expectedData[1]},
            Now()).GetValueSync();
        UNIT_ASSERT_C(!HasError(first), FormatError(first));

        auto second = service->Write(
            {expectedData[2], expectedData[3]},
            Now()).GetValueSync();
        UNIT_ASSERT_C(!HasError(second), FormatError(second));

        UNIT_ASSERT_VALUES_EQUAL(
            expectedData.size(),
            session->GetMessages().size());
        for (size_t i = 0; i != expectedData.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                expectedData[i].Payload,
                session->GetMessages()[i].Payload);
            UNIT_ASSERT_VALUES_EQUAL(
                expectedData[i].SeqNo,
                session->GetMessages()[i].SeqNo);
        }

        service->Stop();
    }

    Y_UNIT_TEST(ShouldHandleSessionError)
    {
        auto session = std::make_shared<TTestWriteSession>(
            NYdb::EStatus::UNAVAILABLE);
        auto service = CreateTestService(std::move(session));
        service->Start();

        auto error = service->Write({TMessage{"hello", 42}}, Now())
            .GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, error.GetCode());
        service->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NLogbroker
