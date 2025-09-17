#include "topic_api.h"

#include <cloud/blockstore/libs/logbroker/iface/config.h>
#include <cloud/blockstore/libs/logbroker/iface/credentials_provider.h>
#include <cloud/blockstore/libs/logbroker/iface/logbroker.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/public/sdk/cpp/client/iam/common/iam.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <util/generic/overloaded.h>
#include <util/stream/file.h>

#include <mutex>
#include <optional>

namespace NCloud::NBlockStore::NLogbroker {

using namespace NThreading;
using namespace NYdb::NTopic;

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

    size_t Written = 0;
    size_t Acknowledged = 0;

    TPromise<NProto::TError> Promise = NewPromise<NProto::TError>();

    TBatch(TVector<TMessage> messages, TInstant timestamp)
        : Messages(std::move(messages))
        , Timestamp(timestamp)
    {}
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

    std::unique_ptr<NYdb::TDriver> Driver;
    std::mutex DriverMutex;

    std::atomic_flag WriteInProgress = false;
    std::shared_ptr<IWriteSession> Session;
    std::unique_ptr<TBatch> Batch;
    std::optional<TContinuationToken> ContinuationToken;

    std::shared_ptr<NYdb::ICredentialsProviderFactory>
        CredentialsProviderFactory;

public:
    TService(
            TLogbrokerConfigPtr config,
            ILoggingServicePtr logging,
            std::shared_ptr<NYdb::ICredentialsProviderFactory>
            credentialsProviderFactory)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
    {}

    TFuture<NProto::TError> Write(
        TVector<TMessage> messages,
        TInstant now) override
    {
        if (messages.empty()) {
            return MakeFuture(MakeError(S_OK));
        }

        if (WriteInProgress.test_and_set()) {
            return MakeFuture(MakeError(E_TRY_AGAIN, "Write in progress"));
        }

        Y_DEBUG_ABORT_UNLESS(!Batch);
        Batch = std::make_unique<TBatch>(std::move(messages), now);

        if (!Session) {
            TTopicClient client{GetDriver()};

            Session = client.CreateWriteSession(
                TWriteSessionSettings()
                    .Path(Config->GetTopic())
                    .ProducerId(Config->GetSourceId())
                    .MessageGroupId(Config->GetSourceId())
                    .RetryPolicy(
                        NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));
        } else if (ContinuationToken.has_value()) {
            TContinuationToken token{std::move(ContinuationToken.value())};
            ContinuationToken.reset();

            WriteNext(*Batch, std::move(token));
        }

        auto future = Batch->Promise.GetFuture();

        WaitEvent();

        return future;
    }

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_LOGBROKER");
    }

    void Stop() override
    {
        std::lock_guard lock{DriverMutex};

        if (Driver) {
            Driver->Stop(false);
            Driver.reset();
        }
    }

private:
    void WaitEvent()
    {
        STORAGE_INFO("Wait event");

        auto self = shared_from_this();
        Session->WaitEvent().Subscribe(
            [self](const auto& future)
            {
                Y_UNUSED(future);
                self->ProcessEvents();
            });
    }

    bool AcksHandler(TBatch& batch, TWriteSessionEvent::TAcksEvent& event)
    {
        STORAGE_DEBUG("Process " << event.DebugString());

        batch.Acknowledged += event.Acks.size();

        return batch.Acknowledged == batch.Messages.size();
    }

    void ReadyToAcceptHander(
        TBatch& batch,
        TWriteSessionEvent::TReadyToAcceptEvent& event)
    {
        STORAGE_DEBUG("Process " << event.DebugString());

        if (batch.Written < batch.Messages.size()) {
            WriteNext(batch, std::move(event.ContinuationToken));
        } else {
            ContinuationToken = std::move(event.ContinuationToken);
        }
    }

    void WriteNext(TBatch& batch, TContinuationToken token)
    {
        STORAGE_INFO("Write next message");

        auto& message = batch.Messages[batch.Written++];

        Session->Write(
            std::move(token),
            message.Payload,
            message.SeqNo,
            batch.Timestamp);
    }

    NProto::TError SessionClosedHandler(const TSessionClosedEvent& event)
    {
        STORAGE_INFO("Process " << event.DebugString());

        auto error = MakeError(
            TranslateErrorCode(event.GetStatus()),
            event.DebugString());

        if (!HasError(error)) {
            // just in case
            error = MakeError(
                E_FAIL,
                TStringBuilder()
                    << "unexpected success: " << event.DebugString());
        }

        return error;
    }

    void ProcessEvents()
    {
        Y_DEBUG_ABORT_UNLESS(Batch);

        std::optional<NProto::TError> error;

        while (TVector events = Session->GetEvents()) {
            for (TWriteSessionEvent::TEvent& event: events) {
                std::visit(TOverloaded {
                    [&] (TWriteSessionEvent::TReadyToAcceptEvent& e) {
                        ReadyToAcceptHander(*Batch, e);
                    },
                    [&] (TWriteSessionEvent::TAcksEvent& e) {
                        if (AcksHandler(*Batch, e)) {
                            error.emplace();
                        }
                    },
                    [&] (const TSessionClosedEvent& e) {
                        error = SessionClosedHandler(e);
                    },
                }, event);
            }

            if (error.has_value()) {
                break;
            }
        }

        if (error.has_value()) {
            STORAGE_INFO("Write is done: " << FormatError(error.value()));

            auto promise = std::move(Batch->Promise);
            Batch.reset();
            WriteInProgress.clear();
            promise.SetValue(std::move(error.value()));

            return;
        }

        WaitEvent();
    }

    const NYdb::TDriver& GetDriver()
    {
        std::lock_guard lock{DriverMutex};
        if (!Driver) {
            Driver = std::make_unique<NYdb::TDriver>(CreateDriverConfig());
        }

        return *Driver;
    }

    NYdb::TDriverConfig CreateDriverConfig() const
    {
        auto cfg = NYdb::TDriverConfig()
            .SetEndpoint(TStringBuilder()
                << Config->GetAddress() << ":" << Config->GetPort())
            .SetDatabase(Config->GetDatabase())
            .SetLog(Logging->CreateLog("BLOCKSTORE_LOGBROKER").ReleaseBackend())
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetDrainOnDtors(true)
            .SetCredentialsProviderFactory(CredentialsProviderFactory);

        if (Config->GetCaCertFilename()) {
            cfg.UseSecureConnection(
                Config->GetCaCertFilename()
                    ? TFileInput(Config->GetCaCertFilename()).ReadAll()
                    : TString());
        }

        return cfg;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging,
    std::shared_ptr<NYdb::ICredentialsProviderFactory>
        credentialsProviderFactory)
{
    return std::make_shared<TService>(
        std::move(config),
        std::move(logging),
        std::move(credentialsProviderFactory));
}

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging)
{
    auto credentialsProviderFactory = CreateCredentialsProviderFactory(*config);

    return CreateTopicAPIService(
        std::move(config),
        std::move(logging),
        std::move(credentialsProviderFactory));
}

}   // namespace NCloud::NBlockStore::NLogbroker
