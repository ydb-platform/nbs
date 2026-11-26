#pragma once

#include <contrib/ydb/library/actors/core/mon.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/http/io/headers.h>
#include <library/cpp/monlib/service/mon_service_http_request.h>

#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////


struct TTestHttpRequest: NMonitoring::IHttpRequest
{
    TCgiParameters Params;
    THttpHeaders HttpHeaders;

    const char* GetURI() const override
    {
        return "";
    }

    const char* GetPath() const override
    {
        return "";
    }

    const TCgiParameters& GetParams() const override
    {
        return Params;
    }

    const TCgiParameters& GetPostParams() const override
    {
        return Params;
    }

    TStringBuf GetPostContent() const override
    {
        return {};
    }

    HTTP_METHOD GetMethod() const override
    {
        return HTTP_METHOD_GET;
    }

    const THttpHeaders& GetHeaders() const override
    {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

// GET request to a monitoring actor page. NMon::TEvHttpInfo only references
// the request, so this object must outlive the response.
class TTestMonHttpRequest final
{
private:
    TTestHttpRequest HttpRequest;
    NMonitoring::TMonService2HttpRequest MonRequest;

public:
    explicit TTestMonHttpRequest(
            TCgiParameters params = {},
            const TString& pathInfo = {})
        : MonRequest(
              nullptr,   // out
              &HttpRequest,
              nullptr,   // monService
              nullptr,   // monPage
              pathInfo,
              nullptr)   // parent
    {
        HttpRequest.Params = std::move(params);
    }

    TTestMonHttpRequest(const TTestMonHttpRequest&) = delete;
    TTestMonHttpRequest& operator=(const TTestMonHttpRequest&) = delete;

    std::unique_ptr<NActors::NMon::TEvHttpInfo> CreateEvent() const
    {
        return std::make_unique<NActors::NMon::TEvHttpInfo>(MonRequest);
    }
};

}   // namespace NCloud::NBlockStore::NStorage
