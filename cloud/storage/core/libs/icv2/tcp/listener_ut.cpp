#include "listener.h"

#include "listener_private.h"

#include <cloud/storage/core/libs/icv2/tcp/testlib/socket_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/system/file.h>

#include <fcntl.h>
#include <poll.h>
#include <sys/resource.h>

#include <cerrno>
#include <future>

namespace NCloud::NIcV2 {

using namespace NTests;
using namespace std::chrono_literals;

namespace {

class TErrorListenerClient final: public IListenerClient
{
public:
    NThreading::TPromise<NProto::TError> Error =
        NThreading::NewPromise<NProto::TError>();
    size_t Connections = 0;

    void OnConnection(TSocketPtr) override
    {
        ++Connections;
    }

    void OnError(const NProto::TError& error) override
    {
        Error.SetValue(error);
    }
};

class TTestErrorInjector final: public NPrivate::IListenerErrorInjector
{
    const int AcceptError;
    const int PrepareError;

public:
    size_t AcceptCalls = 0;
    size_t PrepareCalls = 0;

    explicit TTestErrorInjector(int acceptError = 0, int prepareError = 0)
        : AcceptError(acceptError)
        , PrepareError(prepareError)
    {}

    int BeforeAccept() override
    {
        return AcceptCalls++ == 0 ? AcceptError : 0;
    }

    int BeforePrepareSocket() override
    {
        return PrepareCalls++ == 0 ? PrepareError : 0;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TIcV2ListenerTest)
{
    SIMPLE_UNIT_FORKED_TEST(ReturnsErrorWhenServiceDescriptorsAreExhausted)
    {
        rlimit original;
        UNIT_ASSERT_VALUES_EQUAL(getrlimit(RLIMIT_NOFILE, &original), 0);
        auto limited = original;
        limited.rlim_cur = Min<rlim_t>(original.rlim_cur, 128);
        UNIT_ASSERT_VALUES_EQUAL(setrlimit(RLIMIT_NOFILE, &limited), 0);
        Y_DEFER
        {
            setrlimit(RLIMIT_NOFILE, &original);
        };

        auto client =
            std::make_shared<TTestListenerClient>([](TSocketResult) {});
        for (size_t available: {1u, 3u}) {
            TVector<TFileHandle> files;
            int fd;
            while ((fd = open("/dev/null", O_RDONLY | O_CLOEXEC)) >= 0) {
                files.emplace_back(fd);
            }
            UNIT_ASSERT_VALUES_EQUAL(errno, EMFILE);
            UNIT_ASSERT(files.size() >= available);
            for (size_t i = 0; i < available; ++i) {
                files.pop_back();
            }
            auto result =
                CreateListener(NInterconnect::TAddress("127.0.0.1", 0), client);
            size_t recovered = 0;
            while ((fd = open("/dev/null", O_RDONLY | O_CLOEXEC)) >= 0) {
                files.emplace_back(fd);
                ++recovered;
            }
            files.clear();
            UNIT_ASSERT_VALUES_EQUAL(
                result.GetError().GetCode(),
                MAKE_SYSTEM_ERROR(EMFILE));
            UNIT_ASSERT(!result.GetResult());
            UNIT_ASSERT_VALUES_EQUAL(recovered, available);
        }
    }

    Y_UNIT_TEST(AcceptsPreparedSockets)
    {
        TTestListener listener;
        auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
        listener.Listener->Start();
        connector->Start();
        UNIT_ASSERT(listener.Listener->GetAddress().GetPort());
        auto outgoing = WaitSocket(connector->Connect(
            listener.Listener->GetAddress(),
            TDuration::Seconds(5)));
        auto incoming = listener.Accept();
        CheckSocket(outgoing);
        CheckSocket(incoming);
        connector->Stop();
        listener.Listener->Stop();
        const char sent = 'x';
        UNIT_ASSERT_VALUES_EQUAL(outgoing->Send(&sent, 1, nullptr), 1);
        pollfd readable{
            .fd = *incoming,
            .events = POLLIN,
            .revents = 0,
        };
        UNIT_ASSERT_VALUES_EQUAL(poll(&readable, 1, 1000), 1);
        char received = 0;
        UNIT_ASSERT_VALUES_EQUAL(incoming->Recv(&received, 1, nullptr), 1);
        UNIT_ASSERT_VALUES_EQUAL(received, sent);
    }

    Y_UNIT_TEST(ContinuesAcceptingAfterPendingNetworkErrors)
    {
        constexpr int errors[] = {
            ENETDOWN,
            EPROTO,
            ENOPROTOOPT,
            EHOSTDOWN,
            ENONET,
            EHOSTUNREACH,
            EOPNOTSUPP,
            ENETUNREACH,
            EPERM};
        for (int injectedError: errors) {
            auto firstAccepted = NThreading::NewPromise<TSocketResult>();
            auto secondAccepted = NThreading::NewPromise<TSocketResult>();
            size_t accepted = 0;   // accessed only by the listener worker
            auto injector = std::make_shared<TTestErrorInjector>(injectedError);
            auto [listener, error] = NPrivate::CreateListener(
                NInterconnect::TAddress("127.0.0.1", 0),
                16,
                std::make_shared<TTestListenerClient>(
                    [&](TSocketResult result)
                    {
                        if (accepted++ == 0) {
                            firstAccepted.SetValue(std::move(result));
                        } else {
                            secondAccepted.SetValue(std::move(result));
                        }
                    }),
                injector);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            listener->Start();
            auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
            connector->Start();
            auto first = WaitSocket(connector->Connect(
                listener->GetAddress(),
                TDuration::Seconds(5)));
            auto second = WaitSocket(connector->Connect(
                listener->GetAddress(),
                TDuration::Seconds(5)));
            CheckSocket(WaitSocket(firstAccepted.GetFuture()));
            CheckSocket(WaitSocket(secondAccepted.GetFuture()));
            connector->Stop();
            listener->Stop();
            UNIT_ASSERT(injector->AcceptCalls >= 3);
        }
    }

    Y_UNIT_TEST(ContinuesAcceptingAfterSocketPreparationFailure)
    {
        auto accepted = NThreading::NewPromise<TSocketResult>();
        auto injector = std::make_shared<TTestErrorInjector>(0, EIO);
        auto [listener, error] = NPrivate::CreateListener(
            NInterconnect::TAddress("127.0.0.1", 0),
            16,
            std::make_shared<TTestListenerClient>(
                [accepted](TSocketResult result) mutable
                { accepted.SetValue(std::move(result)); }),
            injector);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
        listener->Start();
        connector->Start();
        auto dropped = WaitResult(
            connector->Connect(listener->GetAddress(), TDuration::Seconds(5)));
        auto outgoing = WaitSocket(
            connector->Connect(listener->GetAddress(), TDuration::Seconds(5)));
        CheckSocket(WaitSocket(accepted.GetFuture()));
        listener->Stop();
        connector->Stop();
        UNIT_ASSERT_VALUES_EQUAL(injector->PrepareCalls, 2);
    }

    Y_UNIT_TEST(ReportsTerminalAcceptError)
    {
        for (int injectedError: {EBADF, EMFILE, ENFILE, ENOBUFS, ENOMEM}) {
            auto client = std::make_shared<TErrorListenerClient>();
            auto injector = std::make_shared<TTestErrorInjector>(injectedError);
            auto [listener, error] = NPrivate::CreateListener(
                NInterconnect::TAddress("127.0.0.1", 0),
                16,
                client,
                injector);
            UNIT_ASSERT_C(!HasError(error), FormatError(error));
            listener->Start();
            auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
            connector->Start();
            auto outgoing = WaitResult(connector->Connect(
                listener->GetAddress(),
                TDuration::Seconds(5)));
            UNIT_ASSERT(client->Error.GetFuture().Wait(TDuration::Seconds(5)));
            listener->Stop();
            connector->Stop();
            UNIT_ASSERT_VALUES_EQUAL(
                client->Error.GetFuture().GetValue().GetCode(),
                MAKE_SYSTEM_ERROR(injectedError));
            UNIT_ASSERT_VALUES_EQUAL(client->Connections, 0);
            UNIT_ASSERT_VALUES_EQUAL(injector->AcceptCalls, 1);
        }
    }

    Y_UNIT_TEST(RejectsDuplicateBindingAndReleasesPort)
    {
        TTestListener first;
        const auto address = first.Listener->GetAddress();
        auto duplicate = CreateListener(
            address,
            std::make_shared<TTestListenerClient>([](TSocketResult) {}));
        UNIT_ASSERT(HasError(duplicate.GetError()));
        first.Listener->Stop();   // covers Stop before Start
        auto [second, error] = CreateListener(
            address,
            std::make_shared<TTestListenerClient>([](TSocketResult) {}));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        second->Start();
        second->Stop();
        second->Stop();
    }

    Y_UNIT_TEST(RejectsInvalidConfiguration)
    {
        auto noClient =
            CreateListener(NInterconnect::TAddress("127.0.0.1", 0), {});
        UNIT_ASSERT_VALUES_EQUAL(noClient.GetError().GetCode(), E_ARGUMENT);
        auto badBacklog = NPrivate::CreateListener(
            NInterconnect::TAddress("127.0.0.1", 0),
            0,
            std::make_shared<TTestListenerClient>([](TSocketResult) {}),
            nullptr);
        UNIT_ASSERT_VALUES_EQUAL(badBacklog.GetError().GetCode(), E_ARGUMENT);
    }

    Y_UNIT_TEST(RequestStopFromCallbackAndConcurrentStop)
    {
        auto entered = NThreading::NewPromise<TSocketResult>();
        auto release = NThreading::NewPromise<void>();
        IListenerPtr listener;
        auto [created, error] = CreateListener(
            NInterconnect::TAddress("127.0.0.1", 0),
            std::make_shared<TTestListenerClient>(
                [&](TSocketResult result)
                {
                    listener->RequestStop();
                    entered.SetValue(std::move(result));
                    release.GetFuture().GetValueSync();
                }));
        UNIT_ASSERT_C(!HasError(error), FormatError(error));
        listener = std::move(created);
        auto connector = NCloud::NIcV2::NTests::CreateTestConnector();
        std::future<void> first;
        std::future<void> second;
        Y_DEFER
        {
            release.TrySetValue();
        };

        listener->Start();
        connector->Start();
        auto socket = WaitSocket(
            connector->Connect(listener->GetAddress(), TDuration::Seconds(5)));
        CheckSocket(WaitSocket(entered.GetFuture()));
        first = std::async(std::launch::async, [&] { listener->Stop(); });
        second = std::async(std::launch::async, [&] { listener->Stop(); });
        const bool waits =
            first.wait_for(50ms) == std::future_status::timeout &&
            second.wait_for(50ms) == std::future_status::timeout;
        release.SetValue();
        first.get();
        second.get();
        connector->Stop();
        UNIT_ASSERT(waits);
    }
}

}   // namespace NCloud::NIcV2
