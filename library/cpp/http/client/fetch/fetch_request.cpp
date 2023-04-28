#include "fetch_request.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <utility>

// TRequest
namespace NHttpFetcher {
    static const TString DEFAULT_ACCEPT_ENCODING = "gzip, deflate";
    constexpr size_t DEFAULT_MAX_HEADER_SIZE = 100 << 10;
    constexpr size_t DEFAULT_MAX_BODY_SIZE = 1 << 29;

    static ui64 GenerateSequence() {
        static TAtomic nextSeq = 0;
        return AtomicIncrement(nextSeq);
    }

    TRequest::TRequest(TString url, TCallBack onFetch)
        : TRequest(std::move(url),
                   DEFAULT_REQUEST_TIMEOUT,
                   DEFAULT_REQUEST_FRESHNESS,
                   false,
                   40,
                   {},
                   {},
                   ELR_RU,
                   std::move(onFetch))
    {
    }

    TRequest::TRequest(TString url, bool ignoreRobotsTxt, TDuration timeout, TDuration freshness, TCallBack onFetch)
        : TRequest(
              std::move(url),
              timeout,
              freshness,
              ignoreRobotsTxt,
              40,
              {},
              {},
              ELR_RU,
              std::move(onFetch))
    {
    }

    TRequest::TRequest(TString url, TDuration timeout, TDuration freshness, bool ignoreRobotsTxt,
                       size_t priority, TMaybe<TString> login, TMaybe<TString> password,
                       ELangRegion langRegion, TCallBack onFetch)
        : Url(std::move(url))
        , Deadline(Now() + timeout)
        , Freshness(freshness)
        , Priority(priority)
        , Login(std::move(login))
        , Password(std::move(password))
        , IgnoreRobotsTxt(ignoreRobotsTxt)
        , LangRegion(langRegion)
        , OnFetch(std::move(onFetch))
        , Sequence(GenerateSequence())
        , AcceptEncoding(DEFAULT_ACCEPT_ENCODING)
        , OnlyHeaders(false)
        , MaxHeaderSize(DEFAULT_MAX_HEADER_SIZE)
        , MaxBodySize(DEFAULT_MAX_BODY_SIZE)
    {
    }

    TRequestRef TRequest::Clone() {
        THolder<TRequest> request = THolder<TRequest>(new TRequest(*this));
        request->Sequence = GenerateSequence();
        return request.Release();
    }

    void TRequest::Dump(IOutputStream& out) {
        out << "url:            " << Url << "\n";
        out << "timeout:        " << (Deadline - Now()).MilliSeconds() << " ms\n";
        out << "freshness:      " << Freshness.Seconds() << "\n";
        out << "priority:       " << Priority << "\n";
        if (!!Login) {
            out << "login:          " << *Login << "\n";
        }
        if (!!Password) {
            out << "password:       " << *Password << "\n";
        }
        if (!!OAuthToken) {
            out << "oauth token:    " << *OAuthToken << "\n";
        }
        if (IgnoreRobotsTxt) {
            out << "ignore robots:  " << IgnoreRobotsTxt << "\n";
        }
        out << "lang reg:       " << LangRegion2Str(LangRegion) << "\n";
        if (!!CustomHost) {
            out << "custom host:    " << *CustomHost << "\n";
        }
        if (!!UserAgent) {
            out << "user agent:     " << *UserAgent << "\n";
        }
        if (!!AcceptEncoding) {
            out << "accept enc:     " << *AcceptEncoding << "\n";
        }
        if (OnlyHeaders) {
            out << "only headers:   " << OnlyHeaders << "\n";
        }
        out << "max header sz:  " << MaxHeaderSize << "\n";
        out << "max body sz:    " << MaxBodySize << "\n";
        if (!!PostData) {
            out << "post data:      " << *PostData << "\n";
        }
        if (!!ContentType) {
            out << "content type:   " << *ContentType << "\n";
        }
    }

}
