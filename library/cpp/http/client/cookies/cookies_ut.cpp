#include "cookie.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NHttp;

Y_UNIT_TEST_SUITE(TestCookiesParser) {
    Y_UNIT_TEST(CheckParseFull) {
        TString hdr = "cookiename=1; expires=Tue, 29 Nov 2016 08:11:54 GMT; Path=/; Domain=yandex.ru; Secure; Httponly;";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "1");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Expires, "Tue, 29 Nov 2016 08:11:54 GMT");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Domain, "yandex.ru");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Path, "/");
        UNIT_ASSERT_VALUES_EQUAL(cookie.IsSecure, true);
        UNIT_ASSERT_VALUES_EQUAL(cookie.IsHttpOnly, true);
    }

    Y_UNIT_TEST(CheckParseEmptyValueSecure) {
        TString hdr = "cookiename=; Secure";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.IsSecure, true);
    }

    Y_UNIT_TEST(CheckParseEmptyValueNotSecure) {
        TString hdr = "cookiename=";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.IsSecure, false);
    }

    Y_UNIT_TEST(CheckParseEmptyValueHttpOnly) {
        TString hdr = "cookiename=; HttpOnly";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.IsHttpOnly, true);
    }

    Y_UNIT_TEST(CheckParseEmptyValueNotHttpOnly) {
        TString hdr = "cookiename=";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.IsHttpOnly, false);
    }

    Y_UNIT_TEST(CheckParsePath) {
        TString hdr = "cookiename=; Path=/testPath";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Domain, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Path, "/testPath");
    }

    Y_UNIT_TEST(CheckParseDomain) {
        TString hdr = "cookiename=; Domain=test.domain";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Domain, "test.domain");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Path, "");
    }

    Y_UNIT_TEST(CheckParseDotDomain) {
        // https://tools.ietf.org/html/rfc6265#section-5.2.3 , front dot is ignored
        TString hdr = "cookiename=; Domain=.test.domain";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Domain, "test.domain");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Path, "");
    }

    Y_UNIT_TEST(CheckParseDomainLowercase) {
        TString hdr = "cookiename=; Domain=Test.Domain.Lower";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Domain, "test.domain.lower");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Path, "");
    }

    Y_UNIT_TEST(CheckParseUnknown) {
        TString hdr = "cookiename=; Something=Other";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
    }

    Y_UNIT_TEST(CheckParseQuotedValue) {
        TString hdr = "cookiename=\"quoted\"";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "\"quoted\"");
    }

    Y_UNIT_TEST(CheckParseStrangeName) {
        TString hdr = "cookiename[1]=a";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename[1]");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "a");
    }

    Y_UNIT_TEST(CheckParseWrongExpires) {
        TString hdr = "cookiename=; Expires=Wrong";
        TCookie cookie;
        UNIT_ASSERT_NO_EXCEPTION(cookie = TCookie::Parse(hdr));
        UNIT_ASSERT_VALUES_EQUAL(cookie.Name, "cookiename");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Value, "");
        UNIT_ASSERT_VALUES_EQUAL(cookie.Expires, "Wrong");
    }

    Y_UNIT_TEST(CheckParseWrongCookieLine) {
        TString hdr = ";;;";
        TCookie cookie;
        UNIT_ASSERT_EXCEPTION(cookie = TCookie::Parse(hdr), yexception);
    }
}
