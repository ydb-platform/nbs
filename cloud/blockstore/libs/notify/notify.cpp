#include "notify.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/http/client/client.h>
#include <library/cpp/http/client/fetch/codes.h>
#include <library/cpp/http/client/ssl/sslsock.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/string/printf.h>

namespace NCloud::NBlockStore::NNotify {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString TMPL_TYPE_NBS_NONREPL_ERROR = "nbs.nonrepl.error";

////////////////////////////////////////////////////////////////////////////////

class TServiceStub final
    : public IService
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TFuture<NProto::TError> NotifyDiskError(
        const TDiskErrorNotification& data) override
    {
        Y_UNUSED(data);

        return MakeFuture(NProto::TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TServiceNull final
    : public IService
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

public:
    explicit TServiceNull(ILoggingServicePtr logging)
        : Logging(std::move(logging))
    {}

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_NOTIFY");
    }

    void Stop() override
    {}

    TFuture<NProto::TError> NotifyDiskError(
        const TDiskErrorNotification& data) override
    {
        STORAGE_WARN("Discard notification "
            << TMPL_TYPE_NBS_NONREPL_ERROR.Quote() << " " << data.UserId.Quote()
            << " " << data.CloudId.Quote() << "/" << data.FolderId.Quote() << " "
            << data.DiskId.Quote());

        return MakeFuture(NProto::TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TService final
    : public IService
{
private:
    const TNotifyConfigPtr Config;

public:
    explicit TService(TNotifyConfigPtr config)
        : Config(std::move(config))
    {}

    void Start() override
    {
        if (auto path = Config->GetCaCertFilename()) {
            NHttpFetcher::TSslSocketBase::LoadCaCerts(path, {});
        }
    }

    void Stop() override
    {}

    TFuture<NProto::TError> NotifyDiskError(
        const TDiskErrorNotification& data) override
    {
        NJson::TJsonMap v {
            { "type", TMPL_TYPE_NBS_NONREPL_ERROR },
            { "data", NJson::TJsonMap {
                { "cloudId", data.CloudId },
                { "folderId", data.FolderId },
                { "diskId", data.DiskId },
            }}
        };

        if (!data.UserId.empty()) {
            v["userId"] = data.UserId;
        } else {
            v["cloudId"] = data.CloudId;
        }

        auto p = NewPromise<NProto::TError>();

        auto query = NHttp::TFetchQuery(
            Config->GetEndpoint(),
            NHttp::TFetchOptions()
                .SetPostData(v.GetStringRobust())
                .SetContentType("application/json")
        );

        FetchAsync(std::move(query), [p, diskId = data.DiskId] (NHttpFetcher::TResultRef rr) mutable {
            if (!rr) {
                p.SetValue(MakeError(E_INVALID_STATE, "unexpected result"));
                return;
            }

            if (NHttpFetcher::IsSuccessCode(rr->Code)) {
                p.SetValue(MakeError(S_OK, Sprintf("HTTP code: %d", rr->Code)));
                return;
            }

            p.SetValue(MakeError(
                E_REJECTED,
                Sprintf(
                    "[%s] can't notify about %s. HTTP error: %d %s",
                    TMPL_TYPE_NBS_NONREPL_ERROR.Quote().c_str(),
                    diskId.Quote().c_str(),
                    rr->Code,
                    rr->Data.c_str()
                )
            ));
        });

        return p.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateService(TNotifyConfigPtr config)
{
    return std::make_shared<TService>(std::move(config));
}

IServicePtr CreateServiceStub()
{
    return std::make_shared<TServiceStub>();
}

IServicePtr CreateNullService(ILoggingServicePtr logging)
{
    return std::make_shared<TServiceNull>(std::move(logging));
}

}   // namespace NCloud::NBlockStore::NNotify
