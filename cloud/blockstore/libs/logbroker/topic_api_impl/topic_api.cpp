#include "topic_api.h"

#include <ydb/public/sdk/cpp/client/iam/common/iam.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>

#include <util/generic/overloaded.h>
#include <util/stream/file.h>

#include <mutex>
#include <optional>

namespace NCloud::NBlockStore::NLogbroker {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

EWellKnownResultCodes TranslateErrorCode(NYdb::EStatus status)
{
    switch (status) {
        case NYdb::EStatus::SUCCESS:
            return S_OK;
        case NYdb::EStatus::ALREADY_EXISTS:
            return S_ALREADY;
        case NYdb::EStatus::UNAUTHORIZED:
            return E_UNAUTHORIZED;
        case NYdb::EStatus::NOT_FOUND:
            return E_NOT_FOUND;
        case NYdb::EStatus::CANCELLED:
            return E_CANCELLED;
        case NYdb::EStatus::BAD_REQUEST:
            return E_ARGUMENT;
        case NYdb::EStatus::UNAVAILABLE:
        case NYdb::EStatus::OVERLOADED:
        case NYdb::EStatus::BAD_SESSION:
        case NYdb::EStatus::SESSION_EXPIRED:
        case NYdb::EStatus::SESSION_BUSY:
            return E_REJECTED;
        default:
            return E_FAIL;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TBatch
{
    TVector<TMessage> Messages;
    TInstant Timestamp;
    std::shared_ptr<NYdb::NTopic::IWriteSession> Session;

    size_t Next = 0;   // index of the next message to be written
    size_t Written = 0;

    TPromise<NProto::TError> Promise = NewPromise<NProto::TError>();

    void AcksHandler(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event)
    {
        Written += event.Acks.size();

        if (Written < Messages.size()) {
            return;
        }

        Session->Close(TDuration {});
    }

    void ReadyToAcceptHander(
        NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event)
    {
        if (Next == Messages.size()) {
            return;
        }

        auto& message = Messages[Next++];

        Session->Write(
            std::move(event.ContinuationToken),
            message.Payload,
            message.SeqNo,
            Timestamp);
    }

    void SessionClosedHandler(const NYdb::NTopic::TSessionClosedEvent& event)
    {
        if (Written != Messages.size()) {
            auto error = MakeError(
                TranslateErrorCode(event.GetStatus()),
                event.DebugString());

            if (!HasError(error)) {
                // just in case
                error = MakeError(
                    E_FAIL,
                    "unexpected success: " + event.DebugString());
            }

            Promise.TrySetValue(std::move(error));
        } else {
            Promise.TrySetValue({});
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TService final
    : public std::enable_shared_from_this<TService>
    , public IService
{
private:
    const TLogbrokerConfigPtr Config;
    const ILoggingServicePtr Logging;
    TLog Log;

    std::optional<NYdb::TDriver> Driver;
    std::mutex DriverMutex;

public:
    TService(
            TLogbrokerConfigPtr config,
            ILoggingServicePtr logging)
        : Config(std::move(config))
        , Logging(std::move(logging))
    {}

    TFuture<NProto::TError> Write(TVector<TMessage> messages, TInstant now) override
    {
        NYdb::NTopic::TTopicClient client {GetDriver()};

        auto batch = std::make_shared<TBatch>();
        batch->Messages = std::move(messages);
        batch->Timestamp = now;
        batch->Session = client.CreateWriteSession(NYdb::NTopic::TWriteSessionSettings()
            .Path(Config->GetTopic())
            .ProducerId(Config->GetSourceId())
            .MessageGroupId(Config->GetSourceId())
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));

        WaitEvent(batch);

        return batch->Promise;
    }

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_LOGBROKER");
    }

    void Stop() override
    {
        std::unique_lock lock {DriverMutex};
        if (Driver) {
            Driver->Stop(false);
            Driver.reset();
        }
    }

private:
    void WaitEvent(std::shared_ptr<TBatch> batch)
    {
        auto self = shared_from_this();
        batch->Session->WaitEvent()
            .Subscribe([self, batch] (const auto& future) {
                Y_UNUSED(future);
                self->ProcessEvents(batch);
            });
    }

    void ProcessEvents(std::shared_ptr<TBatch> batch)
    {
        using namespace NYdb::NTopic;

        bool sessionClosed = false;

        while (TVector events = batch->Session->GetEvents()) {
            for (TWriteSessionEvent::TEvent& event: events) {
                std::visit(TOverloaded {
                    [&] (TWriteSessionEvent::TReadyToAcceptEvent& e) {
                        batch->ReadyToAcceptHander(e);
                    },
                    [&] (TWriteSessionEvent::TAcksEvent& e) {
                        batch->AcksHandler(e);
                    },
                    [&] (const TSessionClosedEvent& e) {
                        batch->SessionClosedHandler(e);
                        sessionClosed = true;
                    },
                }, event);
            }

            if (sessionClosed) {
                break;
            }
        }

        if (!sessionClosed) {
            WaitEvent(std::move(batch));
        }
    }

    NYdb::TDriverConfig CreateDriverConfig() const
    {
        auto cfg = NYdb::TDriverConfig()
            .SetEndpoint(TStringBuilder()
                << Config->GetAddress() << ":" << Config->GetPort())
            .SetDatabase(Config->GetDatabase())
            .SetLog(Logging->CreateLog("BLOCKSTORE_LOGBROKER").ReleaseBackend())
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetDrainOnDtors(true);

        if (TString address = Config->GetMetadataServerAddress()) {
            TStringBuf hostRef;
            TStringBuf portRef;
            TStringBuf{address}.Split(':', hostRef, portRef);

            cfg.SetCredentialsProviderFactory(
                NYdb::CreateIamCredentialsProviderFactory({
                    TString(hostRef),
                    FromString<ui32>(portRef)
                }));

            cfg.UseSecureConnection(
                Config->GetCaCertFilename()
                    ? TFileInput(Config->GetCaCertFilename()).ReadAll()
                    : TString());
        }

        return cfg;
    }

    NYdb::TDriver GetDriver()
    {
        std::unique_lock lock {DriverMutex};

        if (!Driver) {
            Driver.emplace(CreateDriverConfig());
        }

        return *Driver;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging)
{
    return std::make_shared<TService>(std::move(config), std::move(logging));
}

}   // namespace NCloud::NBlockStore::NLogbroker
