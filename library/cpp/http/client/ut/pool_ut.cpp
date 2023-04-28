#include <library/cpp/http/client/fetch/pool.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NHttpFetcher;

static const TString HOST = "https://ya.ru/";
static const TIpPort PORT = 443;

Y_UNIT_TEST_SUITE(TSocketPoolTest) {

    Y_UNIT_TEST(Clear) {
        TSocketPool pool;
        auto s = MakeHolder<TSocketPool::TSocketHandle>();
        pool.ReturnSocket(HOST, PORT, std::move(s));
        pool.Clear();
        UNIT_ASSERT(!pool.GetSocket(HOST, PORT));
    }

    Y_UNIT_TEST(Drain) {
        TSocketPool pool;

        {
            auto s = MakeHolder<TSocketPool::TSocketHandle>();
            pool.ReturnSocket(HOST, PORT, std::move(s));
            pool.Drain(TDuration::Minutes(10));
            UNIT_ASSERT(pool.GetSocket(HOST, PORT));
        }

        {
            auto s = MakeHolder<TSocketPool::TSocketHandle>();
            pool.ReturnSocket(HOST, PORT, std::move(s));
            Sleep(TDuration::MilliSeconds(100));
            pool.Drain(TDuration());
            UNIT_ASSERT(!pool.GetSocket(HOST, PORT));
        }
    }

    Y_UNIT_TEST(GetSet) {
        TSocketPool pool;
        auto s = MakeHolder<TSocketPool::TSocketHandle>();
        pool.ReturnSocket(HOST, PORT, std::move(s));
        UNIT_ASSERT(pool.GetSocket(HOST, PORT));
    }
}
