# Двунаправленное переключение RDMA↔gRPC — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** соединение к хосту ячейки уходит на gRPC при обрыве RDMA и возвращается
на RDMA, когда тот устойчиво поднялся.

**Architecture:** RDMA-клиент получает необязательный `IClientEndpointHandler` и
сообщает о подключении, обрыве и затянувшемся переподключении.
`TTransportSwitcher` из одноразового становится долгоживущим владельцем политики:
он держит обе цели, реализует обработчик через переходник со слабой ссылкой и
двигает `TEndpointRouter::SetTarget` в обе стороны. Возврат на RDMA — через
настраиваемую выдержку.

**Tech Stack:** C++20, Arcadia build (`ya make`), gtest в
`cloud/storage/core/libs/rdma/impl/ut`, `UNITTEST_FOR` в
`cloud/blockstore/libs/cells/impl/ut`.

**Spec:** `docs/superpowers/specs/2026-09-06-cells-bidirectional-transport-switching-design.md`

## Global Constraints

- Сборка и тесты: `./ya make -t <путь до ut>` из корня репозитория. Прогон одной
  цели занимает единицы минут; при первом прогоне после смены базы — дольше.
- Стиль: один параметр на строку, отступ 4, `TAdaptiveLock` для мелких критических
  секций. Новые тесты дописываются **в конец** сьюта, не в середину.
- Коммитить только когда попросят. Шаги «Commit» в задачах выполнять, только если
  об этом сказано отдельно.
- Комментарии в коде — на английском.
- `E_RDMA_UNAVAILABLE` — retriable, повторами занимается `TDurableClient` выше по
  стеку. Ничего похожего в этом плане не реализуем.

---

### Task 1: Обработчик состояния эндпоинта в RDMA-клиенте

**Files:**
- Modify: `cloud/storage/core/libs/rdma/iface/public.h`
- Modify: `cloud/storage/core/libs/rdma/iface/client.h:96-148`
- Modify: `cloud/storage/core/libs/rdma/impl/client.cpp` (член, `StartEndpoint`, три точки вызова)
- Modify: `cloud/blockstore/libs/rdma/fake/client.cpp:1087,1113`
- Modify: `cloud/blockstore/libs/client_rdma/rdma_client_ut.cpp:190`
- Modify: `cloud/blockstore/libs/service_local/storage_rdma_ut.cpp:67`
- Modify: `cloud/blockstore/libs/rdma_test/client_test.h:34`, `client_test.cpp:369`
- Test: `cloud/storage/core/libs/rdma/impl/client_ut.cpp` (в конец файла)

**Interfaces:**
- Produces: `NCloud::NStorage::NRdma::IClientEndpointHandler` с методами
  `HandleConnected(const TString& host, ui32 port)`,
  `HandleDisconnected(const TString& host, ui32 port)`,
  `HandleUnavailable(const TString& host, ui32 port)`;
  `IClientEndpointHandlerPtr = std::shared_ptr<IClientEndpointHandler>`;
  `IClient::StartEndpoint(TString host, ui32 port, IClientEndpointHandlerPtr handler = nullptr)`.

- [ ] **Step 1: Написать падающий тест**

В конец `cloud/storage/core/libs/rdma/impl/client_ut.cpp`, перед закрывающим
`}   // namespace NCloud::NStorage::NRdma`:

```cpp
struct TTestEndpointHandler: public IClientEndpointHandler
{
    TMutex Lock;
    TVector<TString> Events;

    void Add(TString event)
    {
        with_lock (Lock) {
            Events.push_back(std::move(event));
        }
    }

    TVector<TString> GetEvents()
    {
        with_lock (Lock) {
            return Events;
        }
    }

    void HandleConnected(const TString& host, ui32 port) override
    {
        Y_UNUSED(port);
        Add("connected:" + host);
    }

    void HandleDisconnected(const TString& host, ui32 port) override
    {
        Y_UNUSED(port);
        Add("disconnected:" + host);
    }

    void HandleUnavailable(const TString& host, ui32 port) override
    {
        Y_UNUSED(port);
        Add("unavailable:" + host);
    }
};

TEST(TRdmaClientTest, ShouldReportEndpointStateToHandler)
{
    auto testContext = MakeIntrusive<NVerbs::TTestContext>();
    testContext->AllowConnect = true;

    auto verbs = NVerbs::CreateTestVerbs(testContext);
    auto monitoring = CreateMonitoringServiceStub();
    auto clientConfig = std::make_shared<TClientConfig>();

    auto logging =
        CreateLoggingService("console", TLogSettings{TLOG_RESOURCES});

    auto client = CreateTestClient(verbs, logging, monitoring, clientConfig);
    client->Start();
    Y_DEFER
    {
        client->Stop();
    };

    auto handler = std::make_shared<TTestEndpointHandler>();

    auto endpoint =
        client->StartEndpoint("::", 10020, handler).ExtractValueSync();

    // the handler is told after the start future is completed, so the future
    // returning does not yet mean the callback has run
    while (handler->GetEvents().empty()) {
        SpinLockPause();
    }

    ASSERT_EQ("connected:::", handler->GetEvents()[0]);

    Disconnect(testContext);

    while (handler->GetEvents().size() < 2) {
        SpinLockPause();
    }

    ASSERT_EQ("disconnected:::", handler->GetEvents()[1]);
}
```

- [ ] **Step 2: Прогнать тест и убедиться, что он не собирается**

Run: `./ya make -t cloud/storage/core/libs/rdma/impl/ut`
Expected: ошибка компиляции — `IClientEndpointHandler` не объявлен и
`StartEndpoint` не принимает третий аргумент.

- [ ] **Step 3: Объявить интерфейс**

В `cloud/storage/core/libs/rdma/iface/public.h` после блока с `IClientHandler`:

```cpp
struct IClientEndpointHandler;
using IClientEndpointHandlerPtr = std::shared_ptr<IClientEndpointHandler>;
```

В `cloud/storage/core/libs/rdma/iface/client.h` после `IClientHandler`
(строка 104), перед `IClientEndpoint`:

```cpp
////////////////////////////////////////////////////////////////////////////////

// Notifies the user about changes in the endpoint state. The calls arrive on
// the rdma connection manager thread, so an implementation must not block:
// holding that thread up stalls connection management for every endpoint of
// this client.
struct IClientEndpointHandler
{
    virtual ~IClientEndpointHandler() = default;

    // the endpoint is ready to serve requests
    virtual void HandleConnected(const TString& host, ui32 port) = 0;

    // the connection is gone and the requests it carried have been aborted; a
    // reconnect is already scheduled
    virtual void HandleDisconnected(const TString& host, ui32 port) = 0;

    // reconnecting has been failing for long enough to call the endpoint
    // unusable
    virtual void HandleUnavailable(const TString& host, ui32 port) = 0;
};
```

Там же поменять `IClient::StartEndpoint` (строка 141):

```cpp
    virtual NThreading::TFuture<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port,
        IClientEndpointHandlerPtr handler = nullptr) = 0;
```

- [ ] **Step 4: Провести обработчик в реализацию**

В `cloud/storage/core/libs/rdma/impl/client.cpp`, в члены `TClientEndpoint`
рядом с `StartResult` (строка 600):

```cpp
    // set once before the endpoint is attached to the pollers, read from the
    // connection manager thread afterwards
    IClientEndpointHandlerPtr EndpointHandler;
```

В объявлении и определении `TClient::StartEndpoint` (строки 2518 и 2608)
добавить параметр `IClientEndpointHandlerPtr handler`, а в теле — сразу после
создания эндпоинта, до `ConnectionPoller->Attach`:

```cpp
        endpoint->EndpointHandler = std::move(handler);
```

- [ ] **Step 5: Расставить три вызова**

`TClient::HandleConnected`, **после** блока с `StartResult` (строка 2987):

```cpp
    if (endpoint->EndpointHandler) {
        endpoint->EndpointHandler->HandleConnected(
            endpoint->Host,
            endpoint->Port);
    }
```

`TClient::Disconnect`, в самый конец функции (после блока
`if (endpoint->WaitMode == EWaitMode::Poll)`, строка 2829):

```cpp
    if (endpoint->EndpointHandler) {
        endpoint->EndpointHandler->HandleDisconnected(
            endpoint->Host,
            endpoint->Port);
    }
```

`TClient::Reconnect`, в ветке «otherwise keep trying» (строка 2759), то есть
внутри `if (endpoint->Reconnect.Hanging())` после `if (StartResult.Initialized())`:

```cpp
        if (endpoint->EndpointHandler) {
            endpoint->EndpointHandler->HandleUnavailable(
                endpoint->Host,
                endpoint->Port);
        }
```

- [ ] **Step 6: Поправить пять реализаций `IClient`**

В каждой добавить третий параметр и не использовать его:
`cloud/storage/core/libs/rdma/impl/client.cpp` (уже сделано в шаге 4),
`cloud/blockstore/libs/rdma/fake/client.cpp:1087` и `:1113`,
`cloud/blockstore/libs/client_rdma/rdma_client_ut.cpp:190`,
`cloud/blockstore/libs/service_local/storage_rdma_ut.cpp:67`,
`cloud/blockstore/libs/rdma_test/client_test.h:34` вместе с
`client_test.cpp:369`. Образец:

```cpp
    TFuture<NRdma::IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port,
        NRdma::IClientEndpointHandlerPtr handler) override
    {
        Y_UNUSED(handler);
        // ... тело без изменений
    }
```

- [ ] **Step 7: Прогнать тест и убедиться, что он проходит**

Run: `./ya make -t cloud/storage/core/libs/rdma/impl/ut`
Expected: PASS, включая `ShouldReportEndpointStateToHandler`.

- [ ] **Step 8: Проверить, что тест не пустой**

Временно убрать вызов `HandleConnected` из шага 5, прогнать тест — он должен
упасть на `ASSERT_EQ(1u, handler->GetEvents().size())`. Вернуть вызов, прогнать
снова — PASS.

- [ ] **Step 9: Собрать всех потребителей**

Run: `./ya make cloud/blockstore/libs/rdma/fake cloud/blockstore/libs/rdma_test cloud/blockstore/libs/service_local cloud/blockstore/libs/client_rdma cloud/blockstore/libs/storage/disk_agent`
Expected: Ok.

---

### Task 2: Параметр выдержки в конфиге ячейки

**Files:**
- Modify: `cloud/blockstore/config/cells.proto:43-48`
- Modify: `cloud/blockstore/libs/cells/iface/config.cpp:14-26,128-142`
- Modify: `cloud/blockstore/libs/cells/iface/config.h:21-100`
- Test: `cloud/blockstore/libs/cells/impl/ut` (косвенно, через Task 4)

**Interfaces:**
- Produces: `TCellConfig::GetRdmaSettleTime()` и
  `TCellHostConfig::GetRdmaSettleTime()`, оба возвращают `TDuration`.

- [ ] **Step 1: Добавить поле в прото и исправить устаревший комментарий**

В `cloud/blockstore/config/cells.proto`, в `message TCellConfig`:

```proto
    // While the RDMA data endpoint is unavailable - either because it has not
    // been set up yet or because the connection broke - serve data over gRPC.
    optional bool GrpcDataFallbackEnabled = 11;

    // How long an RDMA connection has to stay up before data is moved back
    // onto it. Zero switches over as soon as it connects.
    optional uint32 RdmaSettleTimeMs = 12;
```

Прежний комментарий у `GrpcDataFallbackEnabled` утверждал, что переключение на
RDMA никогда не отменяется — с этой задачей он становится неправдой, поэтому
заменяется целиком.

- [ ] **Step 2: Завести геттер в конфиге ячейки**

В `cloud/blockstore/libs/cells/iface/config.cpp`, в
`BLOCKSTORE_CELL_DEFAULT_CONFIG`, после строки с `GrpcDataFallbackEnabled`:

```cpp
    xxx(RdmaSettleTimeMs,            ui32,                   30000            )\
```

В `cloud/blockstore/libs/cells/iface/config.h` в объявления `TCellConfig`
добавить:

```cpp
    [[nodiscard]] ui32 GetRdmaSettleTimeMs() const;
```

- [ ] **Step 3: Пробросить в `TCellHostConfig`**

В `config.h` в приватные члены `TCellHostConfig` после `GrpcDataFallbackEnabled`:

```cpp
    TDuration RdmaSettleTime;
```

и публичный геттер:

```cpp
    TDuration GetRdmaSettleTime() const
    {
        return RdmaSettleTime;
    }
```

В `config.cpp` в список инициализации `TCellHostConfig::TCellHostConfig`:

```cpp
    , RdmaSettleTime(TDuration::MilliSeconds(cellConfig.GetRdmaSettleTimeMs()))
```

- [ ] **Step 4: Собрать**

Run: `./ya make -t cloud/blockstore/libs/cells/impl/ut`
Expected: Ok, тесты по-прежнему проходят.

---

### Task 3: Переключатель становится долгоживущим и двунаправленным

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/transport_switcher.h`
- Modify: `cloud/blockstore/libs/cells/impl/transport_switcher.cpp`
- Test: `cloud/blockstore/libs/cells/impl/transport_switcher_ut.cpp` (в конец сьюта)

**Interfaces:**
- Consumes: `IClientEndpointHandler` из Task 1; `TDuration` выдержки из Task 2.
- Produces:
  ```cpp
  struct ITransportSwitcher
  {
      virtual ~ITransportSwitcher() = default;
      virtual NCloud::NStorage::NRdma::IClientEndpointHandlerPtr
          GetEndpointHandler() = 0;
  };
  using ITransportSwitcherPtr = std::shared_ptr<ITransportSwitcher>;

  ITransportSwitcherPtr StartTransportSwitching(
      IEndpointRouterPtr router,
      TEndpointFactory factory,
      ITimerPtr timer,
      ISchedulerPtr scheduler,
      ILoggingServicePtr logging,
      TString host,
      TTransportSwitcherConfig config);
  ```
  где `TTransportSwitcherConfig` получает поле `TDuration SettleTime`.

- [ ] **Step 1: Написать падающие тесты**

В `transport_switcher_ut.cpp`, в `TTestEnv`, добавить поле и правку хелпера:

```cpp
    TDuration SettleTime = TDuration::Seconds(10);
    ITransportSwitcherPtr Switcher;
```

Фабрики стенда получают параметр обработчика — он им не нужен, но сигнатура
`TEndpointFactory` меняется в этой же задаче:

```cpp
    TEndpointFactory AlwaysSucceeds()
    {
        return FailsThenSucceeds(0);
    }

    TEndpointFactory FailsThenSucceeds(ui32 failures)
    {
        return [this, failures](
                   NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler)
        {
            Y_UNUSED(handler);
            ++FactoryCalls;

            if (FactoryCalls <= failures) {
                return MakeFuture(TResultOrError<IBlockStorePtr>(
                    MakeError(E_REJECTED, "endpoint is not up yet")));
            }

            return MakeFuture(TResultOrError<IBlockStorePtr>(Better));
        };
    }
```

и в `StartSwitching` заменить возврат:

```cpp
        Switcher = StartTransportSwitching(
            Router,
            Initial,        // the endpoint the router starts on
            std::move(factory),
            Timer,
            Scheduler,
            Logging,
            "test-host",
            TTransportSwitcherConfig{
                .InitialRetryDelay = TDuration::Seconds(1),
                .MaxRetryDelay = TDuration::Seconds(4),
                .SettleTime = SettleTime,
            });
```

Тесты в конец сьюта `TTransportSwitcherTest`:

```cpp
    Y_UNIT_TEST(ShouldSwitchToRdmaOnlyAfterItSettles)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected("test-host", 10020);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);

        env.AdvanceTime(TDuration::Seconds(10));

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldSwitchToRdmaAtOnceWhenSettleTimeIsZero)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        env.Switcher->GetEndpointHandler()->HandleConnected("test-host", 10020);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldNotSwitchWhenRdmaBreaksWhileSettling)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected("test-host", 10020);

        env.AdvanceTime(TDuration::Seconds(5));
        handler->HandleDisconnected("test-host", 10020);
        env.AdvanceTime(TDuration::Seconds(10));

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldFallBackToGrpcWhenRdmaBreaks)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected("test-host", 10020);
        handler->HandleDisconnected("test-host", 10020);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Initial->ReadCount);
        UNIT_ASSERT_VALUES_EQUAL(0, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldReturnToRdmaAfterItComesBack)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected("test-host", 10020);
        handler->HandleDisconnected("test-host", 10020);
        handler->HandleConnected("test-host", 10020);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldIgnoreUnavailable)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected("test-host", 10020);
        handler->HandleUnavailable("test-host", 10020);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldTolerateRepeatedConnected)
    {
        TTestEnv env;
        env.SettleTime = TDuration::Zero();
        env.StartSwitching(env.AlwaysSucceeds());

        auto handler = env.Switcher->GetEndpointHandler();
        handler->HandleConnected("test-host", 10020);
        handler->HandleConnected("test-host", 10020);

        Read(env.Router);
        UNIT_ASSERT_VALUES_EQUAL(1, env.Better->ReadCount);
    }

    Y_UNIT_TEST(ShouldNotSettleOntoAReleasedRouter)
    {
        TTestEnv env;
        env.StartSwitching(env.AlwaysSucceeds());

        env.Switcher->GetEndpointHandler()->HandleConnected("test-host", 10020);
        env.Router.reset();

        // the settle timer must find the router gone and do nothing
        env.AdvanceTime(TDuration::Seconds(10));
    }
```

- [ ] **Step 2: Прогнать и убедиться, что не собирается**

Run: `./ya make -t cloud/blockstore/libs/cells/impl/ut`
Expected: ошибка компиляции — нет `ITransportSwitcher`, `GetEndpointHandler`,
поля `SettleTime`.

- [ ] **Step 3: Объявить интерфейс переключателя**

В `transport_switcher.h` заменить объявление функции и добавить тип. Комментарий
про односторонность удалить — он перестаёт быть правдой:

```cpp
struct TTransportSwitcherConfig
{
    TDuration InitialRetryDelay = TDuration::Seconds(1);
    TDuration MaxRetryDelay = TDuration::Seconds(30);
    TDuration SettleTime = TDuration::Seconds(30);
};

// The handler has to reach the rdma client, and only the switcher can make it,
// so the factory is handed one rather than capturing it.
using TEndpointFactory = std::function<NThreading::TFuture<
    TResultOrError<IBlockStorePtr>>(
    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr)>;

////////////////////////////////////////////////////////////////////////////////

// Decides which transport the router points at, for as long as the connection
// lives. Data starts on the endpoint the router was created with and moves onto
// the preferred transport once the factory has produced it and it has stayed
// connected for SettleTime; a break moves the data straight back.
//
// Immediate on the way down and deliberate on the way up: at a break there is
// nothing to wait for since the requests are already aborted, while returning on
// the first reconnect would keep feeding a flapping link.
//
// The handler from GetEndpointHandler() must be given to the rdma client so it
// reports the endpoint state. It holds the switcher weakly, so the switcher's
// life is bound by whoever owns it and not by the endpoint.
struct ITransportSwitcher
{
    virtual ~ITransportSwitcher() = default;

    virtual NCloud::NStorage::NRdma::IClientEndpointHandlerPtr
        GetEndpointHandler() = 0;
};

using ITransportSwitcherPtr = std::shared_ptr<ITransportSwitcher>;

ITransportSwitcherPtr StartTransportSwitching(
    IEndpointRouterPtr router,
    IBlockStorePtr fallback,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString host,
    TTransportSwitcherConfig config);
```

Добавить `#include <cloud/storage/core/libs/rdma/iface/client.h>`.

- [ ] **Step 4: Реализовать политику**

В `transport_switcher.cpp` заменить класс. Ключевые отличия от нынешнего: он
больше не умирает после успеха, хранит добытый эндпоинт, а переключением
управляют колбэки.

```cpp
class TTransportSwitcher final
    : public ITransportSwitcher
    , public std::enable_shared_from_this<TTransportSwitcher>
{
private:
    const std::weak_ptr<IEndpointRouter> Router;
    const IBlockStorePtr Fallback;   // the endpoint the router started with
    const TEndpointFactory Factory;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const TString Host;
    const TDuration SettleTime;

    TLog Log;
    TBackoffDelayProvider RetryDelay;

    TAdaptiveLock Lock;
    IBlockStorePtr Preferred;      // the rdma endpoint, once acquired
    bool PreferredActive = false;  // is the router pointing at it
    bool Connected = false;
    ui64 SettleGeneration = 0;

public:
    TTransportSwitcher(
            IEndpointRouterPtr router,
            IBlockStorePtr fallback,
            TEndpointFactory factory,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            const ILoggingServicePtr& logging,
            TString host,
            const TTransportSwitcherConfig& config)
        : Router(std::move(router))
        , Fallback(std::move(fallback))
        , Factory(std::move(factory))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Host(std::move(host))
        , SettleTime(config.SettleTime)
        , Log(logging->CreateLog("BLOCKSTORE_CELLS"))
        , RetryDelay(config.InitialRetryDelay, config.MaxRetryDelay)
    {}

    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr GetEndpointHandler()
        override
    {
        return std::make_shared<TEndpointHandler>(weak_from_this());
    }

    // unchanged from the current file except for the factory call, which now
    // gets the handler: Factory(GetEndpointHandler()).Subscribe(...)
    void Attempt()
    {
        if (Router.expired()) {
            return;
        }

        Factory(GetEndpointHandler())
            .Subscribe(
                [self = shared_from_this()](const auto& future)
                { self->OnAttemptCompleted(future.GetValue()); });
    }

    void OnConnected()
    {
        ui64 generation = 0;

        with_lock (Lock) {
            Connected = true;
            generation = ++SettleGeneration;
        }

        if (!SettleTime) {
            Settle(generation);
            return;
        }

        Scheduler->Schedule(
            Timer->Now() + SettleTime,
            [weakSelf = weak_from_this(), generation]
            {
                if (auto self = weakSelf.lock()) {
                    self->Settle(generation);
                }
            });
    }

    void OnDisconnected()
    {
        auto router = Router.lock();

        with_lock (Lock) {
            Connected = false;
            // invalidates a settle in flight
            ++SettleGeneration;

            if (!PreferredActive) {
                return;
            }
            PreferredActive = false;
        }

        STORAGE_INFO("[" << Host << "] moving data back onto the fallback");

        if (router) {
            router->SetTarget(Fallback);
        }
    }

private:
    void Settle(ui64 generation)
    {
        IBlockStorePtr preferred;

        with_lock (Lock) {
            if (generation != SettleGeneration || !Connected ||
                PreferredActive || !Preferred)
            {
                return;
            }
            PreferredActive = true;
            preferred = Preferred;
        }

        auto router = Router.lock();
        if (!router) {
            return;
        }

        STORAGE_INFO("[" << Host << "] switched over to the preferred transport");
        router->SetTarget(std::move(preferred));
    }
};
```

`Fallback` — это `IBlockStorePtr`, с которым создан роутер; передаётся в
конструктор переключателя из Task 4. `OnAttemptCompleted` при успехе больше не
зовёт `SetTarget`, а лишь запоминает эндпоинт:

```cpp
        if (!HasError(result) && result.GetResult()) {
            with_lock (Lock) {
                Preferred = result.GetResult();
            }
            return;
        }
```

Переходник-обработчик, разрывающий цикл:

```cpp
class TEndpointHandler final
    : public NCloud::NStorage::NRdma::IClientEndpointHandler
{
private:
    const std::weak_ptr<TTransportSwitcher> Switcher;

public:
    explicit TEndpointHandler(std::weak_ptr<TTransportSwitcher> switcher)
        : Switcher(std::move(switcher))
    {}

    void HandleConnected(const TString& host, ui32 port) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        if (auto self = Switcher.lock()) {
            self->OnConnected();
        }
    }

    void HandleDisconnected(const TString& host, ui32 port) override
    {
        Y_UNUSED(host);
        Y_UNUSED(port);
        if (auto self = Switcher.lock()) {
            self->OnDisconnected();
        }
    }

    void HandleUnavailable(const TString& host, ui32 port) override
    {
        Y_UNUSED(port);
        // nothing to do: by now the data is already on the fallback. The signal
        // belongs to host liveness, which is a separate concern.
        STORAGE_WARN("[" << host << "] rdma endpoint is unavailable");
    }
};
```

`StartTransportSwitching` возвращает созданный переключатель вместо `void`.

- [ ] **Step 5: Прогнать тесты**

Run: `./ya make -t cloud/blockstore/libs/cells/impl/ut`
Expected: PASS, включая все восемь новых тестов и шесть существующих.

- [ ] **Step 6: Проверить, что тесты не пустые**

Убрать выдержку — сделать `Settle` безусловным при `HandleConnected` — и
убедиться, что падает `ShouldNotSwitchWhenRdmaBreaksWhileSettling`. Убрать
`SetTarget(Fallback)` из `OnDisconnected` — должен упасть
`ShouldFallBackToGrpcWhenRdmaBreaks`. Вернуть обе правки.

---

### Task 4: Проводка — соединение владеет переключателем

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/endpoint_bootstrap.h`
- Modify: `cloud/blockstore/libs/cells/impl/endpoint_bootstrap_impl.cpp`
- Modify: `cloud/blockstore/libs/client_rdma/rdma_client.h`, `rdma_client.cpp:949-971`
- Modify: `cloud/blockstore/libs/cells/impl/connection.cpp:158-232`
- Test: `cloud/blockstore/libs/cells/impl/connection_ut.cpp` (в конец сьюта)

**Interfaces:**
- Consumes: `ITransportSwitcherPtr` и `GetEndpointHandler()` из Task 3;
  `IClientEndpointHandlerPtr` из Task 1.
- Produces: `TCellConnection` с третьим полем `ITransportSwitcherPtr Switcher`.

- [ ] **Step 1: Написать падающий тест**

В `connection_ut.cpp` в `TTestEndpointBootstrap` добавить запоминание
обработчика:

```cpp
    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr RdmaHandler;
```

и в `SetupHostRdmaEndpoint` — сохранение переданного обработчика. В конец сьюта
`TCellConnectionTest`:

```cpp
    Y_UNIT_TEST(ShouldMoveDataBackToGrpcWhenRdmaBreaks)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, true);

        auto connection = env.Connect("host-a");

        env.EndpointsSetup->RdmaSetupPromise.SetValue(
            TResultOrError<IBlockStorePtr>(env.RdmaService));

        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);
        env.EndpointsSetup->RdmaHandler->HandleConnected("host-a", 10020);

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaService->RequestCount);

        env.EndpointsSetup->RdmaHandler->HandleDisconnected("host-a", 10020);

        const auto grpcRequests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(1, env.RdmaService->RequestCount);
        UNIT_ASSERT_VALUES_EQUAL(
            grpcRequests + 1,
            env.GrpcClient->Service->RequestCount);
    }
```

В `TTestEnv` выставить нулевую выдержку через конфиг ячейки:
`proto.SetRdmaSettleTimeMs(0);`.

- [ ] **Step 2: Прогнать и убедиться, что не собирается**

Run: `./ya make -t cloud/blockstore/libs/cells/impl/ut`
Expected: ошибка компиляции — у `SetupHostRdmaEndpoint` нет параметра
обработчика.

- [ ] **Step 3: Пробросить обработчик через client_rdma**

В `cloud/blockstore/libs/client_rdma/rdma_client.h` у
`CreateRdmaDataEndpointAsync` добавить параметр:

```cpp
NThreading::TFuture<TResultOrError<IBlockStorePtr>> CreateRdmaDataEndpointAsync(
    ILoggingServicePtr logging,
    NCloud::NStorage::NRdma::IClientPtr client,
    ITraceSerializerPtr traceSerializer,
    ITaskQueuePtr taskQueue,
    const TRdmaEndpointConfig& config,
    NCloud::NStorage::NRdma::IClientEndpointHandlerPtr handler = nullptr);
```

и в `rdma_client.cpp:962` передать его в `client->StartEndpoint`.

- [ ] **Step 4: Пробросить через bootstrap ячейки**

В `endpoint_bootstrap.h` у `ICellHostEndpointBootstrap::SetupHostRdmaEndpoint`
добавить третий параметр `NCloud::NStorage::NRdma::IClientEndpointHandlerPtr
handler`, в `endpoint_bootstrap_impl.cpp` передать его в
`CreateRdmaDataEndpointAsync`.

- [ ] **Step 5: Соединение владеет переключателем**

В `connection.cpp` `CreateSwitchingDataEndpoint` начинает отдавать пару. Порядок
важен: переключатель создаётся до того, как фабрика будет вызвана, иначе
обработчика ещё нет.

```cpp
struct TSwitchingDataEndpoint
{
    IBlockStorePtr Router;
    ITransportSwitcherPtr Switcher;
};

TSwitchingDataEndpoint CreateSwitchingDataEndpoint(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService)
{
    auto fallback = CreateGrpcDataEndpoint(bootstrap, hostConfig, controlService);
    auto router = CreateEndpointRouter(fallback);

    auto switcher = StartTransportSwitching(
        router,
        fallback,
        [bootstrap, hostConfig](auto handler)
        {
            return bootstrap.EndpointsSetup->SetupHostRdmaEndpoint(
                bootstrap,
                hostConfig,
                std::move(handler));
        },
        bootstrap.Timer,
        bootstrap.Scheduler,
        bootstrap.Logging,
        hostConfig.GetFqdn(),
        TTransportSwitcherConfig{
            .SettleTime = hostConfig.GetRdmaSettleTime(),
        });

    return {std::move(router), std::move(switcher)};
}
```

`TEndpointFactory` становится
`std::function<TFuture<TResultOrError<IBlockStorePtr>>(IClientEndpointHandlerPtr)>`,
и `Attempt()` вызывает её как `Factory(GetEndpointHandler())`.

`SetupDataEndpoint` меняет тип результата на
`TFuture<TResultOrError<TSwitchingDataEndpoint>>` (для веток без переключателя
`Switcher` остаётся пустым), `TCellConnection` получает поле
`const ITransportSwitcherPtr Switcher;` и заполняет его в конструкторе,
`CreateCellConnection` пробрасывает.

- [ ] **Step 6: Прогнать тесты**

Run: `./ya make -t cloud/blockstore/libs/cells/impl/ut`
Expected: PASS.

- [ ] **Step 7: Собрать всё затронутое**

Run: `./ya make cloud/blockstore/apps/server cloud/blockstore/tools/testing/loadtest cloud/blockstore/libs/client_rdma`
Expected: Ok.

- [ ] **Step 8: Проверить, что сквозной тест не пустой**

Временно вернуть `OnDisconnected` без `SetTarget` — должен упасть
`ShouldMoveDataBackToGrpcWhenRdmaBreaks`. Вернуть.
