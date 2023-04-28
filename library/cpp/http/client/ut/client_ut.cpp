#include <library/cpp/http/client/client.h>
#include <library/cpp/http/client/fetch/codes.h>

#include <library/cpp/http/server/http.h>
#include <library/cpp/http/fetch/exthttpcodes.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/fetch/exthttpcodes.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/network/socket.h>
#include <util/stream/input.h>
#include <util/system/event.h>

using namespace NHttpFetcher;
using namespace NHttp;

class TTestServer: public THttpServer, public THttpServer::ICallBack {
public:
    using THandler = std::function<THttpResponse()>;

    TTestServer()
        : THttpServer{this, TOptions(TPortManager{}.GetTcpPort()).AddBindAddress("localhost")}
    {
        Address_ = TStringBuilder() << "http://localhost:" << Options().Port;
        Start();
    }

    ~TTestServer() {
        Stop();
    }

    void AddHandler(TString path, THandler resp) {
        Handlers_[path] = resp;
    }

    const TString& Address() const noexcept {
        return Address_;
    }

private:
    TClientRequest* CreateClient() override {
        struct TReplier: public TRequestReplier {
            explicit TReplier(const THandlers& handlers)
                : Handlers_{handlers} {
            }

            bool DoReply(const TReplyParams& r) override {
                static auto notFoundHandler = [] {
                    THttpResponse resp;
                    resp.SetHttpCode(HttpCodes::HTTP_NOT_FOUND);
                    return resp;
                };

                TParsedHttpRequest req{r.Input.FirstLine()};

                auto it = Handlers_.find(req.Request);
                if (it == Handlers_.end()) {
                    r.Output << notFoundHandler();
                    return true;
                }

                r.Output << it->second();
                return true;
            }

            const THandlers& Handlers_;
        };

        return new TReplier{Handlers_};
    }

private:
    using THandlers = THashMap<TString, THandler>;
    THandlers Handlers_;
    TString Address_;
};

class THttpClientTest: public TTestBase {
public:
    UNIT_TEST_SUITE(THttpClientTest);
    UNIT_TEST(TestReadTimeout);
    UNIT_TEST(TestTimeoutOnRetry);
    UNIT_TEST(TestPartialReadCancel);
    UNIT_TEST(TestCounter);
    UNIT_TEST(TestCancellation);
    UNIT_TEST(TestCanStopMultipleCoroutines);
    UNIT_TEST(TestInvokeCallbackForEarlyCancellation);
    UNIT_TEST_SUITE_END();

    void SetUp() override {
        Srv_.AddHandler("/slow", [] {
            THttpResponse resp;
            Sleep(TDuration::MilliSeconds(500));
            return resp;
        });

        Srv_.AddHandler("/counter", [] {
            static ui64 cnt = 0;

            THttpResponse resp;
            resp.SetContent(TStringBuilder() << cnt++);

            return resp;
        });

        Srv_.AddHandler("/slow_counter", [] {
            static ui64 cnt = 0;

            THttpResponse resp;
            Sleep(TDuration::MilliSeconds(50));
            resp.SetContent(TStringBuilder() << cnt++);

            return resp;
        });

        Srv_.AddHandler("/data", [] {
            struct: IInputStream {
                size_t DoRead(void* buf, size_t len) override {
                    const size_t toRead = Min(len, Rem_);
                    std::generate_n(static_cast<char*>(buf), toRead, [] { return '0'; });
                    Rem_ -= toRead;

                    return toRead;
                }

                size_t Limit_{1 * 1024 * 1024};
                size_t Rem_{Limit_};
            } stream;

            THttpResponse resp;
            resp.SetContent(stream.ReadAll());
            return resp;
        });

        Srv_.AddHandler("/always_failing", [] {
            THttpResponse resp;
            Sleep(TDuration::MilliSeconds(10));
            resp.SetHttpCode(HTTP_INTERNAL_SERVER_ERROR);
            return resp;
        });
    }

    void TestReadTimeout() {
        TFetchClient client;

        struct TContext {
            TAutoEvent done;
            int code{0};
        };

        TAtomicSharedPtr<TContext> ctx{new TContext};

        auto state = client.Fetch(
            TFetchQuery{
                Srv_.Address() + "/slow",
                TFetchOptions{}.SetTimeout(TDuration::MilliSeconds(40))},
            [=] (TResultRef result)
        {
            ctx->code = result->Code;
            ctx->done.Signal();
        });

        bool signaled = ctx->done.WaitT(TDuration::MilliSeconds(100));
        UNIT_ASSERT(signaled);
        UNIT_ASSERT_EQUAL(ctx->code, HTTP_TIMEDOUT_WHILE_BYTES_RECEIVING);
    }

    void TestTimeoutOnRetry() {
        TFetchClient client;

        int code {0};
        bool finished = client.Fetch(
            TFetchQuery{
                Srv_.Address() + "/always_failing",
                TFetchOptions{}.SetTimeout(TDuration::MilliSeconds(20))
                    .SetRetryCount(10)
                    .SetRetryDelay(TDuration::MilliSeconds(10))},
            [&code] (TResultRef result) {
                code = result->Code;
            }).WaitT(TDuration::Seconds(50));

        UNIT_ASSERT(finished);
    }

    void TestPartialReadCancel() {
        TFetchClient client;

        struct TContext {
            TAutoEvent done;
            int code{0};
        };

        TAtomicSharedPtr<TContext> ctx{new TContext};

        auto partialReadHandler = [] (const TString&) {
            static int i{0};

            if (i++ < 2) {
                return true;
            }

            return false;
        };

        int code{0};
        client.Fetch(
            TFetchQuery{
                Srv_.Address() + "/data"
            }.OnPartialRead(partialReadHandler),
            [&] (TResultRef result)
        {
            code = result->Code;
        }).WaitI();

        UNIT_ASSERT_EQUAL(code, HTTP_BODY_TOO_LARGE);
    }

    void TestCounter() {
        TFetchQuery query{Srv_.Address() + "/counter"};

        ui64 reqCnt = 3;
        TVector<TString> data;
        data.reserve(reqCnt);

        for (size_t i = 0; i != reqCnt; ++i) {
            auto result = Fetch(query);
            data.emplace_back(result->Data);
        }

        UNIT_ASSERT_EQUAL(data.size(), reqCnt);
        UNIT_ASSERT_EQUAL(data[0], "0");
        UNIT_ASSERT_EQUAL(data[1], "1");
        UNIT_ASSERT_EQUAL(data[2], "2");
    }

    void TestCancellation() {
        NHttp::TClientOptions options;
        options.SetFetchCoroutines(1);
        TFetchClient client(options);

        TFetchQuery query{Srv_.Address() + "/slow_counter"};
        auto state1 = client.Fetch(query, [=](const TResultRef&){});
        auto state2 = client.Fetch(query, [=](const TResultRef&){});
        auto state3 = client.Fetch(query, [=](const TResultRef&){});

        state2.Cancel();
        state3.WaitI();
        UNIT_ASSERT(state3.Get());
        UNIT_ASSERT_EQUAL(state3.Get()->Data, "1");
    }

    void TestCanStopMultipleCoroutines() {
        for (size_t i = 0; i < 100; ++i) {
            NHttp::TClientOptions options;
            options.SetFetchCoroutines(3);
            TFetchClient client(options);
            TFetchQuery query{Srv_.Address() + "/slow_counter"};
            auto state = client.Fetch(query, [=](const TResultRef&){});
            state.Cancel();
        }
    }

    void TestInvokeCallbackForEarlyCancellation() {
        for (size_t i = 0; i < 100; ++i) {
            NHttp::TClientOptions options;
            options.SetFetchCoroutines(3);
            TFetchClient client(options);
            TFetchQuery query{Srv_.Address() + "/slow_counter"};
            TAutoEvent done{};
            int code = 0;
            auto state = client.Fetch(query, [&](const TResultRef& result) {
                code = result->Code;
                done.Signal();
            });
            state.Cancel();
            done.Wait();
            UNIT_ASSERT(code == FETCH_CANCELLED || code == HTTP_OK);
        }
    }

    TTestServer Srv_;
};

UNIT_TEST_SUITE_REGISTRATION(THttpClientTest);
