#include <library/cpp/http/client/fetch/parse.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NHttpFetcher;

struct TService {
    THttpURL::EKind Kind;
    TString Host;
    ui16 Port;
};

static bool DoParseUrl(const TStringBuf& url, TService* service) {
    ::ParseUrl(TString(url), service->Kind, service->Host, service->Port);
    return true;
}

Y_UNIT_TEST_SUITE(TParseTest) {
    static void Check(const TString& url, THttpURL::EKind kind, TString host, ui16 port) {
        TService s;

        UNIT_ASSERT(DoParseUrl(url, &s));
        UNIT_ASSERT_EQUAL(s.Kind, kind);
        UNIT_ASSERT_EQUAL(s.Host, host);
        UNIT_ASSERT_EQUAL(s.Port, port);
    }

    Y_UNIT_TEST(TestParseUrl) {
        Check("http://ya.ru/", THttpURL::SchemeHTTP, "ya.ru", 80);
        Check("http://ya.ru:8080/", THttpURL::SchemeHTTP, "ya.ru", 8080);
        Check("https://ya.ru/", THttpURL::SchemeHTTPS, "ya.ru", 443);
        Check("https://ya.ru:1120/", THttpURL::SchemeHTTPS, "ya.ru", 1120);

        Check("http://127.0.0.1/", THttpURL::SchemeHTTP, "127.0.0.1", 80);
        Check("http://127.0.0.1:81/", THttpURL::SchemeHTTP, "127.0.0.1", 81);
    }

    Y_UNIT_TEST(TestParseIDN) {
        Check("https://яндекс.рф/", THttpURL::SchemeHTTPS, "xn--d1acpjx3f.xn--p1ai", 443);
    }
}
