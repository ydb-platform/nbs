#include "notify.h"

#include "https.h"
#include "json_generator.h"

#include <cloud/blockstore/libs/notify/iface/config.h>
#include <cloud/blockstore/libs/notify/iface/notify.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NNotify {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TService final
    : public IService
    , public std::enable_shared_from_this<TService>
{
private:
    const TNotifyConfigPtr Config;
    NCloud::NIamClient::IIamTokenClientPtr IamClient;
    THttpsClient HttpsClient;
    TLog Log;
    IJsonGeneratorPtr JsonGenerator;

public:
    TService(
            TNotifyConfigPtr config,
            NCloud::NIamClient::IIamTokenClientPtr iamClient,
            IJsonGeneratorPtr jsonGenerator)
        : Config(std::move(config))
        , IamClient(std::move(iamClient))
        , JsonGenerator(std::move(jsonGenerator))
    {}

    void Start() override
    {
        if (auto path = Config->GetCaCertFilename()) {
            HttpsClient.LoadCaCerts(path);
        }
    }

    void Stop() override
    {}

    auto GetIamToken()
    {
        if (Config->GetVersion() == 2) {
            if (!IamClient) {
                STORAGE_WARN(
                    "missing iam-client "
                    << "Got error while requesting token: "
                    << "IAM client is missing");
            } else {
                return IamClient->GetTokenAsync().Apply(
                    [weakPtr = weak_from_this()](const auto& future) -> TResultOrError<TString>
                    {
                        const auto& response = future.GetValue();

                        if (HasError(response)) {
                            return response.GetError();
                        }

                        auto tokenInfo = response.GetResult();
                        if (tokenInfo.Token.empty()) {
                            auto self = weakPtr.lock();
                            if (self) {
                                auto& Log = self->Log;
                                STORAGE_WARN(
                                    "missing iam-token "
                                    << "Got error while requesting token: "
                                    << "iam token is empty");
                            }
                            return MakeError(E_ARGUMENT, "empty iam token");
                        };

                        return std::move(tokenInfo.Token);
                    });
            }
        }
        return MakeFuture(TResultOrError<TString>(TString()));
    }

    TFuture<NProto::TError> Notify(const TNotification& data) override
    {
        auto p = NewPromise<NProto::TError>();

        GetIamToken().Subscribe(
            [weakPtr = weak_from_this(),
             p,
             event = data.Event,
             v = JsonGenerator->Generate(data)](
                TFuture<TResultOrError<TString>> future) mutable
            {
                auto [token, error] = future.ExtractValue();
                if (HasError(error)) {
                    p.SetValue(error);
                    return;
                }
                auto self = weakPtr.lock();

                if (!self) {
                    p.SetValue(MakeError(
                        E_REJECTED,
                        "Object of the Notify class was destroyed before request sending"));
                    return;
                }

                self->HttpsClient.Post(
                    self->Config->GetEndpoint(),
                    v.GetStringRobust(),
                    "application/json",
                    token,
                    [p, event](int code, const TString& message) mutable
                    {
                        const bool isSuccess = code >= 200 && code < 300;

                        if (isSuccess) {
                            p.SetValue(MakeError(S_OK, TStringBuilder()
                                << "HTTP code: " << code));
                            return;
                        }

                        p.SetValue(MakeError(E_REJECTED, TStringBuilder()
                                << "Couldn't send notification " << event
                                << ". HTTP error: " << code << " " << message));
                    });
            });

        return p.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateService(
    TNotifyConfigPtr config,
    NCloud::NIamClient::IIamTokenClientPtr iamTokenClientPtr,
    IJsonGeneratorPtr jsonGenerator)
{
    return std::make_shared<TService>(
        std::move(config),
        std::move(iamTokenClientPtr),
        std::move(jsonGenerator));
}

IServicePtr CreateService(
    TNotifyConfigPtr config,
    NCloud::NIamClient::IIamTokenClientPtr iamTokenClientPtr)
{
    return CreateService(
        std::move(config),
        std::move(iamTokenClientPtr),
        std::make_unique<TJsonGenerator>());
}

}   // namespace NCloud::NBlockStore::NNotify
