#include "cookiestore.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NHttp;

Y_UNIT_TEST_SUITE(TestCookieStore) {
    Y_UNIT_TEST(CheckSingle) {
        TCookieStore store;
        NUri::TUri uri;
        uri.Parse("http://yandex.ru");
        UNIT_ASSERT(store.SetCookie(uri, "c1=v1"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri), "c1=v1");
    }

    Y_UNIT_TEST(CheckSingleExpired) {
        TCookieStore store;
        NUri::TUri uri;
        uri.Parse("http://yandex.ru");
        UNIT_ASSERT(store.SetCookie(uri, "c1=v1;expires=Sun, 06 Nov 1994 08:49:37 GMT"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri), "");
    }

    Y_UNIT_TEST(CheckSingleExpiredRelaxed) {
        TCookieStore store;
        NUri::TUri uri;
        uri.Parse("http://yandex.ru");
        UNIT_ASSERT(store.SetCookie(uri, "c1=v1;expires=Thu, 01-Jan-1970 00:00:01 GMT"));
        // Это некорректный expires (из-за "-" в дате), и мы по RFC такой expires должны
        // игнорировать. Но блинк его парсит, и получается, что кука уже expired/
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri), "");
    }

    Y_UNIT_TEST(CheckTwoChangeValue) {
        TCookieStore store;
        NUri::TUri uri;
        uri.Parse("http://yandex.ru");
        UNIT_ASSERT(store.SetCookie(uri, "c1=v1"));
        UNIT_ASSERT(store.SetCookie(uri, "c1=v2"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri), "c1=v2");
    }

    Y_UNIT_TEST(CheckSingleCase) {
        TCookieStore store;
        NUri::TUri uri1, uri2;
        uri1.Parse("http://yandex.RU");
        uri2.Parse("http://yandex.ru");
        UNIT_ASSERT(store.SetCookie(uri1, "c1=v1"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri2), "c1=v1");
    }

    Y_UNIT_TEST(CheckTwo) {
        TCookieStore store;
        NUri::TUri uri;
        uri.Parse("http://yandex.ru");
        UNIT_ASSERT(store.SetCookie(uri, "c1=v1"));
        UNIT_ASSERT(store.SetCookie(uri, "c2=v2"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri), "c1=v1; c2=v2");
    }

    Y_UNIT_TEST(CheckThreeSorted) {
        TCookieStore store;
        NUri::TUri uri1, uri2;
        uri1.Parse("http://yandex.ru/");
        uri2.Parse("http://yandex.ru/long/path");
        UNIT_ASSERT(store.SetCookie(uri1, "c1=v1"));
        UNIT_ASSERT(store.SetCookie(uri1, "c2=v2;path=/long"));
        UNIT_ASSERT(store.SetCookie(uri1, "c3=v3;path=/long/path"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri2), "c3=v3; c2=v2; c1=v1");
    }

    Y_UNIT_TEST(CheckOtherPath) {
        TCookieStore store;
        NUri::TUri uri1, uri2;
        uri1.Parse("http://yandex.ru/path1");
        uri2.Parse("http://yandex.ru/path2");
        UNIT_ASSERT(store.SetCookie(uri1, "c1=v1"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri2), "");
    }

    Y_UNIT_TEST(CheckSubdomain) {
        TCookieStore store;
        NUri::TUri uri1, uri2;
        uri1.Parse("http://yandex.ru/");
        uri2.Parse("http://sub.yandex.ru/");
        UNIT_ASSERT(store.SetCookie(uri1, "c1=v1"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri2), "");
    }

    Y_UNIT_TEST(CheckSubdomainAllowed) {
        TCookieStore store;
        NUri::TUri uri1, uri2;
        uri1.Parse("http://yandex.ru/");
        uri2.Parse("http://sub.yandex.ru/");
        UNIT_ASSERT(store.SetCookie(uri1, "c1=v1;domain=yandex.ru"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri2), "c1=v1");
    }

    Y_UNIT_TEST(CheckSubdomainAllowed2) {
        TCookieStore store;
        NUri::TUri uri1, uri2;
        uri1.Parse("http://sub.yandex.ru/");
        uri2.Parse("http://yandex.ru/");
        UNIT_ASSERT(store.SetCookie(uri1, "c1=v1;domain=yandex.ru"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri2), "c1=v1");
    }

    Y_UNIT_TEST(CheckTwoAlmostEqual) {
        TCookieStore store;
        NUri::TUri uri1, uri2;
        uri1.Parse("http://hh.ru/");
        uri2.Parse("http://m.hh.ru/");
        UNIT_ASSERT(store.SetCookie(uri1, "_xsrf=30c2f234ae19f2ca86167783bf09f708; Domain=.hh.ru; Path=/"));
        UNIT_ASSERT(store.SetCookie(uri1, "_xsrf=30c2f234ae19f2ca86167783bf09f708; Path=/"));
        UNIT_ASSERT_VALUES_EQUAL(store.GetCookieString(uri2), "_xsrf=30c2f234ae19f2ca86167783bf09f708");
    }
}
