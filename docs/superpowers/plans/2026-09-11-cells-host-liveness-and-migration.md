# Cells Host Liveness and Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** соединение ячейки переезжает на другой хост, когда его хост перестал отвечать на `Ping` или когда маунт сообщил, что тайблет тома живёт в другом месте.

**Architecture:** `TCellConnection` перестаёт быть привязанным к хосту навсегда. Он держит два долгоживущих роутера — control и data, — а всё, что относится к конкретному хосту, собрано в `THostBinding`, которую переезд заменяет целиком. Живость хостов меряет пингер внутри `TCellHostPool`; он же дёргает соединения через узкий интерфейс `ICellHostWatcher`.

**Tech Stack:** C++20, Arcadia build (`ya make`), `THotSwap` для wait-free подмены цели роутера, `TAdaptiveLock`, `ITimer`/`IScheduler` для периодических задач, `TTestScheduler`/`TTestTimer` в тестах.

**Spec:** `docs/superpowers/specs/2026-09-11-cells-host-liveness-and-migration-design.md`

## Global Constraints

- Комментарии в коде — на английском. Русский только в этом плане и в спеке.
- Новые тесты добавляются **в конец** файла, а не в середину.
- `ColumnLimit: 80`, сборка с `-Werror`.
- Ничего не коммитить без явной просьбы пользователя. Шаги «Commit» в задачах —
  исключение, согласованное для работы по плану на отдельной ветке.
- Сборка и тесты: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 <path>` из корня
  репозитория. Один тест: добавить `-F "TSuiteName::TestName"`.
- Никаких спекулятивных механизмов: если сценарий нельзя назвать, код не пишется.

---

## File Structure

| Файл | Ответственность |
|---|---|
| `impl/endpoint_router.h` | плюс `ITransportTarget` — узкий вид на роутер, только `SetTarget` |
| `impl/detachable_target.{h,cpp}` | **новый**: пересылает `SetTarget` в роутер, пока не отцеплен |
| `impl/detachable_target_ut.cpp` | **новый**: его тесты |
| `impl/transport_switcher.{h,cpp}` | переключатель принимает `ITransportTarget` вместо роутера |
| `config/cells.proto`, `iface/config.{h,cpp}` | три новых поля конфига |
| `impl/host_pool.{h,cpp}` | `ICellHostWatcher`, регистрация наблюдателей, пингер |
| `impl/host_pool_ut.cpp` | тесты наблюдателей и пингера |
| `impl/connection.cpp` | роутеры, `THostBinding`, переезд, оба повода |
| `impl/connection_ut.cpp` | тесты переезда |
| `impl/ya.make`, `impl/ut/ya.make` | новые файлы в сборку |

---

### Task 1: Узкий интерфейс цели для переключателя

Переключателю не нужен весь `IBlockStore` роутера — только `SetTarget`. Узкий
интерфейс нужен, чтобы между переключателем и роутером можно было поставить
отцепляемую прослойку (Task 2).

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/endpoint_router.h`
- Modify: `cloud/blockstore/libs/cells/impl/transport_switcher.h`
- Modify: `cloud/blockstore/libs/cells/impl/transport_switcher.cpp`

**Interfaces:**
- Consumes: ничего.
- Produces: `ITransportTarget` с единственным методом
  `virtual void SetTarget(IBlockStorePtr target) = 0;` и алиасом
  `using ITransportTargetPtr = std::shared_ptr<ITransportTarget>;`.
  `IEndpointRouter` начинает наследоваться от него, поэтому
  `IEndpointRouterPtr` по-прежнему подходит везде, где ждут `ITransportTargetPtr`.
  `StartTransportSwitching` первым параметром принимает `ITransportTargetPtr`.

- [ ] **Step 1: Сузить интерфейс в `endpoint_router.h`**

Заменить объявление `IEndpointRouter` на:

```cpp
// The part of a router that decides what it points at. Kept separate so that
// something else can stand between a chooser and the router it drives.
struct ITransportTarget
{
    virtual ~ITransportTarget() = default;

    virtual void SetTarget(IBlockStorePtr target) = 0;
};

using ITransportTargetPtr = std::shared_ptr<ITransportTarget>;

////////////////////////////////////////////////////////////////////////////////

struct IEndpointRouter
    : public IBlockStore
    , public ITransportTarget
{
};
```

Комментарий, который сейчас стоит над `IEndpointRouter`, остаётся на месте —
он описывает роутер, а не сужение.

- [ ] **Step 2: Переключатель принимает цель, а не роутер**

В `transport_switcher.h` заменить первый параметр:

```cpp
ITransportSwitcherPtr StartTransportSwitching(
    ITransportTargetPtr target,
    IBlockStorePtr fallback,
    TEndpointFactory factory,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString host,
    TTransportSwitcherConfig config);
```

В `transport_switcher.cpp` заменить тип поля и параметра конструктора:

```cpp
    const std::weak_ptr<ITransportTarget> Router;
```

переименовать поле в `Target` и поправить три места, где оно читается
(`Router.expired()` в `Start`, `Router.lock()` в `OnDisconnected` и в `Settle`),
на `Target`. Имя локальной переменной `router` заменить на `target`.

- [ ] **Step 3: Собрать и прогнать существующие тесты**

Это чистый рефакторинг: поведение не меняется, и проверяют его уже написанные
тесты переключателя, которые передают роутер напрямую (он теперь наследник
`ITransportTarget`, конверсия неявная).

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: PASS, 38 тестов.

- [ ] **Step 4: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/endpoint_router.h \
        cloud/blockstore/libs/cells/impl/transport_switcher.h \
        cloud/blockstore/libs/cells/impl/transport_switcher.cpp
git commit -m "cells: narrow what the transport switcher sees of the router"
```

---

### Task 2: Отцепляемая цель

Роутер переживёт привязку к хосту, а переключатель — нет. Между ними нужна
прослойка, которую переезд отцепляет: иначе переключатель покинутого хоста,
подняв RDMA через секунду после свапа, утянет данные на мёртвый хост.

**Files:**
- Create: `cloud/blockstore/libs/cells/impl/detachable_target.h`
- Create: `cloud/blockstore/libs/cells/impl/detachable_target.cpp`
- Test: `cloud/blockstore/libs/cells/impl/detachable_target_ut.cpp`
- Modify: `cloud/blockstore/libs/cells/impl/ya.make`
- Modify: `cloud/blockstore/libs/cells/impl/ut/ya.make`

**Interfaces:**
- Consumes: `ITransportTarget`, `ITransportTargetPtr` из Task 1.
- Produces:
  ```cpp
  struct IDetachableTarget: public ITransportTarget
  {
      virtual void Detach() = 0;
  };
  using IDetachableTargetPtr = std::shared_ptr<IDetachableTarget>;

  IDetachableTargetPtr CreateDetachableTarget(ITransportTargetPtr target);
  ```

- [ ] **Step 1: Написать падающий тест**

Создать `detachable_target_ut.cpp`:

```cpp
#include "detachable_target.h"

#include "endpoint_router.h"

#include <cloud/blockstore/libs/service/service_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRecordingTarget: public ITransportTarget
{
    TVector<IBlockStorePtr> Targets;

    void SetTarget(IBlockStorePtr target) override
    {
        Targets.push_back(std::move(target));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDetachableTargetTest)
{
    Y_UNIT_TEST(ShouldForwardUntilDetached)
    {
        auto recording = std::make_shared<TRecordingTarget>();
        auto detachable = CreateDetachableTarget(recording);

        auto first = std::make_shared<TTestService>();
        detachable->SetTarget(first);

        UNIT_ASSERT_VALUES_EQUAL(1, recording->Targets.size());
        UNIT_ASSERT(recording->Targets[0] == first);
    }

    Y_UNIT_TEST(ShouldStaySilentAfterDetach)
    {
        auto recording = std::make_shared<TRecordingTarget>();
        auto detachable = CreateDetachableTarget(recording);

        detachable->Detach();
        detachable->SetTarget(std::make_shared<TTestService>());

        UNIT_ASSERT_VALUES_EQUAL(0, recording->Targets.size());
    }

    Y_UNIT_TEST(ShouldReleaseTargetOnDetach)
    {
        auto recording = std::make_shared<TRecordingTarget>();
        std::weak_ptr<TRecordingTarget> weak = recording;

        auto detachable = CreateDetachableTarget(recording);
        recording.reset();

        UNIT_ASSERT_C(weak.lock(), "the target must be held until detached");

        detachable->Detach();
        UNIT_ASSERT_C(!weak.lock(), "detaching must release the target");
    }
}

}   // namespace NCloud::NBlockStore::NCells
```

- [ ] **Step 2: Прогнать и убедиться, что не собирается**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: FAIL — `detachable_target.h` не существует.

- [ ] **Step 3: Написать реализацию**

`detachable_target.h`:

```cpp
#pragma once

#include "endpoint_router.h"

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

// Passes SetTarget on to the target it was created with, until it is detached.
// After that it does nothing, forever.
//
// Stands between a transport switcher and the router of a connection: the
// router outlives the host the switcher belongs to, and a switcher left over
// from an abandoned host must not be able to point the router back at it.
struct IDetachableTarget: public ITransportTarget
{
    virtual void Detach() = 0;
};

using IDetachableTargetPtr = std::shared_ptr<IDetachableTarget>;

IDetachableTargetPtr CreateDetachableTarget(ITransportTargetPtr target);

}   // namespace NCloud::NBlockStore::NCells
```

`detachable_target.cpp`:

```cpp
#include "detachable_target.h"

#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDetachableTarget final: public IDetachableTarget
{
private:
    TAdaptiveLock Lock;
    ITransportTargetPtr Target;

public:
    explicit TDetachableTarget(ITransportTargetPtr target)
        : Target(std::move(target))
    {}

    void SetTarget(IBlockStorePtr target) override
    {
        // under the lock, so that a detach racing this call either lets it
        // through whole or stops it entirely
        with_lock (Lock) {
            if (Target) {
                Target->SetTarget(std::move(target));
            }
        }
    }

    void Detach() override
    {
        with_lock (Lock) {
            Target.reset();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDetachableTargetPtr CreateDetachableTarget(ITransportTargetPtr target)
{
    return std::make_shared<TDetachableTarget>(std::move(target));
}

}   // namespace NCloud::NBlockStore::NCells
```

- [ ] **Step 4: Добавить файлы в сборку**

В `impl/ya.make` в `SRCS` добавить `detachable_target.cpp` (список
отсортирован по алфавиту — вставить после `connection.cpp`).
В `impl/ut/ya.make` в `SRCS` добавить `detachable_target_ut.cpp` (после
`describe_volume_ut.cpp`).

- [ ] **Step 5: Прогнать тесты**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut -F "TDetachableTargetTest::*"`
Expected: PASS, 3 теста.

- [ ] **Step 6: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/detachable_target.h \
        cloud/blockstore/libs/cells/impl/detachable_target.cpp \
        cloud/blockstore/libs/cells/impl/detachable_target_ut.cpp \
        cloud/blockstore/libs/cells/impl/ya.make \
        cloud/blockstore/libs/cells/impl/ut/ya.make
git commit -m "cells: add a detachable transport target"
```

---

### Task 3: Поля конфига

**Files:**
- Modify: `cloud/blockstore/config/cells.proto`
- Modify: `cloud/blockstore/libs/cells/iface/config.cpp`
- Modify: `cloud/blockstore/libs/cells/iface/config.h`
- Test: `cloud/blockstore/libs/cells/impl/host_pool_ut.cpp`

**Interfaces:**
- Produces: `TCellConfig::GetHostMigrationEnabled() -> bool`,
  `TCellConfig::GetHostPingPeriod() -> TDuration`,
  `TCellConfig::GetHostPingTimeout() -> TDuration`, и
  `TCellHostConfig::GetHostMigrationEnabled() -> bool`.

Пул читает период и таймаут из `TCellConfig`, который у него есть. А соединение
видит только `TCellHostConfig`, поэтому флаг доезжает до него тем же путём, что
`GrpcDataFallbackEnabled` и `RdmaSettleTime`, — копированием в конструкторе
`TCellHostConfig`.

- [ ] **Step 1: Написать падающий тест**

Добавить **в конец** `host_pool_ut.cpp`, внутрь `Y_UNIT_TEST_SUITE`:

```cpp
    Y_UNIT_TEST(ShouldHaveLivenessDefaults)
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        TCellConfig config(std::move(proto));

        // off by default: the whole feature is gated by one switch
        UNIT_ASSERT(!config.GetHostMigrationEnabled());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(5),
            config.GetHostPingPeriod());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(2),
            config.GetHostPingTimeout());
    }

    Y_UNIT_TEST(ShouldPassMigrationFlagToHostConfig)
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        proto.SetHostMigrationEnabled(true);
        proto.AddHosts()->SetFqdn("host-a");
        TCellConfig config(std::move(proto));

        // a connection only ever sees the host config
        const auto& host = *config.GetHosts().FindPtr("host-a");
        UNIT_ASSERT(host.GetHostMigrationEnabled());
    }
```

- [ ] **Step 2: Прогнать и убедиться, что падает**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut -F "TCellHostPoolTest::ShouldHaveLivenessDefaults"`
Expected: FAIL — таких методов нет.

- [ ] **Step 3: Добавить поля в proto**

В `cells.proto`, в `message TCellConfig`, после `RdmaSettleTime = 12`:

```proto
    // Watch the hosts of this cell over gRPC and move a connection off a host
    // that stopped answering. Also moves it to the host where the volume
    // tablet lives, when a mount reports one.
    optional bool HostMigrationEnabled = 13;

    // How often every known host of the cell is pinged, in milliseconds.
    optional uint32 HostPingPeriod = 14;

    // How long a ping may take before the host counts as dead, in
    // milliseconds.
    optional uint32 HostPingTimeout = 15;
```

- [ ] **Step 4: Добавить поля в конфиг**

В `iface/config.cpp`, в `BLOCKSTORE_CELL_DEFAULT_CONFIG`, после строки
`RdmaSettleTime`:

```cpp
    xxx(HostMigrationEnabled,        bool,                   false            )\
    xxx(HostPingPeriod,              TDuration,   TDuration::Seconds(5)      )\
    xxx(HostPingTimeout,             TDuration,   TDuration::Seconds(2)      )\
```

В `iface/config.h`, в список объявлений `TCellConfig` (там, где
`GetRdmaSettleTime`):

```cpp
    [[nodiscard]] bool GetHostMigrationEnabled() const;
    [[nodiscard]] TDuration GetHostPingPeriod() const;
    [[nodiscard]] TDuration GetHostPingTimeout() const;
```

Конвертация миллисекунд в `TDuration` уже есть — специализация
`ConvertValue<TDuration, ui32>` в `config.cpp`.

- [ ] **Step 4b: Протянуть флаг в `TCellHostConfig`**

В `iface/config.h`, в приватные поля `TCellHostConfig`, рядом с
`GrpcDataFallbackEnabled`:

```cpp
    bool HostMigrationEnabled = false;
```

и геттер рядом с `GetGrpcDataFallbackEnabled`:

```cpp
    bool GetHostMigrationEnabled() const
    {
        return HostMigrationEnabled;
    }
```

В `iface/config.cpp`, в список инициализации конструктора `TCellHostConfig`,
после `RdmaSettleTime`:

```cpp
    , HostMigrationEnabled(cellConfig.GetHostMigrationEnabled())
```

- [ ] **Step 5: Прогнать тест**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut -F "TCellHostPoolTest::ShouldHaveLivenessDefaults"`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cloud/blockstore/config/cells.proto \
        cloud/blockstore/libs/cells/iface/config.cpp \
        cloud/blockstore/libs/cells/iface/config.h \
        cloud/blockstore/libs/cells/impl/host_pool_ut.cpp
git commit -m "cells: add host liveness config"
```

---

### Task 4: Наблюдатели хоста в пуле

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/host_pool.h`
- Modify: `cloud/blockstore/libs/cells/impl/host_pool.cpp`
- Test: `cloud/blockstore/libs/cells/impl/host_pool_ut.cpp`

**Interfaces:**
- Produces:
  ```cpp
  struct ICellHostWatcher
  {
      virtual ~ICellHostWatcher() = default;
      virtual void OnHostUnavailable() noexcept = 0;
  };
  using ICellHostWatcherPtr = std::shared_ptr<ICellHostWatcher>;
  ```
  и две новые операции пула, отдельные от взятия канала:
  ```cpp
  void WatchHost(const TString& fqdn, ICellHostWatcherPtr watcher);
  void UnwatchHost(const TString& fqdn, const ICellHostWatcherPtr& watcher);
  ```

Наблюдение отделено от владения каналом сознательно: соединение регистрируется
уже после того, как построено (раньше `shared_from_this()` недоступен), а при
переезде подписка и канал меняются в разные моменты. Сигнатуры
`AcquireControlChannel`/`ReleaseControlChannel` остаются прежними, и вызывающих
править не нужно.

- [ ] **Step 1: Поставить дубли, без которых пул не собрать в тесте**

`AcquireControlChannel` идёт в `Bootstrap.EndpointsSetup`, а существующие тесты
пула конструируют его пустым (`TBootstrap{}`) и потому никогда не берут канал.
Значит, дубли нужны здесь. Добавить в анонимный namespace `host_pool_ut.cpp`,
после `MakeCellConfig`:

```cpp
////////////////////////////////////////////////////////////////////////////////

struct TPingableService: public TTestService
{
    ui32 Pings = 0;
    NProto::TError PingError;

    TPingableService()
    {
        PingHandler = [this](std::shared_ptr<NProto::TPingRequest> request)
        {
            Y_UNUSED(request);
            ++Pings;

            NProto::TPingResponse response;
            *response.MutableError() = PingError;
            return MakeFuture(std::move(response));
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestMultiClientEndpoint
    : public TBlockStoreImpl<
          TTestMultiClientEndpoint,
          NClient::IMultiClientEndpoint>
{
    const std::shared_ptr<TPingableService> Service;

    explicit TTestMultiClientEndpoint(
            std::shared_ptr<TPingableService> service)
        : Service(std::move(service))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    IBlockStorePtr CreateClientEndpoint(
        const TString& clientId,
        const TString& instanceId) override
    {
        Y_UNUSED(clientId);
        Y_UNUSED(instanceId);
        return Service;
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        return TMethod::Execute(
            Service.get(),
            std::move(callContext),
            std::move(request));
    }
};

```

Инклюды в начале файла:

```cpp
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
```

`TTestService` уже несёт `PingHandler`: обработчики генерируются макросом
`BLOCKSTORE_SERVICE` для каждого метода сервиса.

И вспомогательная функция для тестов этой задачи, туда же:

```cpp
TBootstrap MakeBootstrap(std::shared_ptr<TTestEndpointBootstrap> endpoints)
{
    TBootstrap bootstrap;
    bootstrap.EndpointsSetup = std::move(endpoints);
    bootstrap.Logging = CreateLoggingService("console");
    return bootstrap;
}
```

- [ ] **Step 2: Написать падающие тесты**

Добавить **в конец** `host_pool_ut.cpp`:

```cpp
    Y_UNIT_TEST(ShouldNotifyWatchersWhenHostDies)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable() noexcept override
            {
                ++Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-a");
        pool.WatchHost("host-a", watcher);

        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(1, watcher->Notifications);

        // a host that is dead and stays dead keeps saying so: a connection
        // whose migration failed has to get another chance
        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(2, watcher->Notifications);

        pool.SetHostAlive("host-a", true);
        UNIT_ASSERT_VALUES_EQUAL(2, watcher->Notifications);
    }

    Y_UNIT_TEST(ShouldNotNotifyWatchersOfOtherHosts)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable() noexcept override
            {
                ++Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-a");
        pool.WatchHost("host-a", watcher);

        pool.SetHostAlive("host-b", false);
        UNIT_ASSERT_VALUES_EQUAL(0, watcher->Notifications);
    }

    Y_UNIT_TEST(ShouldForgetReleasedWatcher)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable() noexcept override
            {
                ++Notifications;
            }
        };

        TCellHostPool pool(
            MakeCellConfig(),
            MakeBootstrap(std::make_shared<TTestEndpointBootstrap>()));

        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-a");
        pool.WatchHost("host-a", watcher);
        pool.UnwatchHost("host-a", watcher);

        pool.SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL(0, watcher->Notifications);
    }

    Y_UNIT_TEST(ShouldTrackLivenessOfDiscoveredHost)
    {
        struct TWatcher: public ICellHostWatcher
        {
            ui32 Notifications = 0;

            void OnHostUnavailable() noexcept override
            {
                ++Notifications;
            }
        };

        TCellHostPool pool(MakeCellConfig(), TBootstrap{});

        // host-z is not configured: it is the tablet host of somebody's
        // volume, and a connection sits on it all the same
        auto watcher = std::make_shared<TWatcher>();
        pool.AcquireControlChannel("host-z");
        pool.WatchHost("host-z", watcher);

        pool.SetHostAlive("host-z", false);
        UNIT_ASSERT_VALUES_EQUAL(1, watcher->Notifications);
    }
```

- [ ] **Step 3: Прогнать и убедиться, что не собирается**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: FAIL — нет `ICellHostWatcher`, у методов другая арность.

- [ ] **Step 4: Объявить интерфейс и поменять сигнатуры**

В `host_pool.h`, перед `class TCellHostPool`:

```cpp
////////////////////////////////////////////////////////////////////////////////

// Told when the host it is attached to stops answering.
//
// Called from the pinger's thread, so an implementation must not block: the
// same sweep serves every host of the cell.
//
// The pool holds watchers weakly, so releasing a control channel is enough to
// stop being told - and a watcher that is gone is dropped on the next sweep.
struct ICellHostWatcher
{
    virtual ~ICellHostWatcher() = default;

    virtual void OnHostUnavailable() noexcept = 0;
};

using ICellHostWatcherPtr = std::shared_ptr<ICellHostWatcher>;
```

В `struct TChannel` добавить поле:

```cpp
        TVector<std::weak_ptr<ICellHostWatcher>> Watchers;
```

и объявить два публичных метода рядом с `SetHostAlive`:

```cpp
    void WatchHost(const TString& fqdn, ICellHostWatcherPtr watcher);
    void UnwatchHost(const TString& fqdn, const ICellHostWatcherPtr& watcher);
```

Для `TVector` понадобится `#include <util/generic/vector.h>`.

- [ ] **Step 5: Реализовать в `host_pool.cpp`**

Подписка и отписка — обе под тем же локом, что и остальное состояние канала:

```cpp
void TCellHostPool::WatchHost(const TString& fqdn, ICellHostWatcherPtr watcher)
{
    if (!watcher) {
        return;
    }

    with_lock (Lock) {
        // only for a channel that exists: watching a host nobody talks to
        // would leak the subscription
        if (auto* channel = Channels.FindPtr(fqdn)) {
            channel->Watchers.push_back(std::move(watcher));
        }
    }
}

void TCellHostPool::UnwatchHost(
    const TString& fqdn,
    const ICellHostWatcherPtr& watcher)
{
    with_lock (Lock) {
        if (auto* channel = Channels.FindPtr(fqdn)) {
            EraseIf(
                channel->Watchers,
                [&](const auto& w) { return w.lock() == watcher; });
        }
    }
}
```

`SetHostAlive` снимает флаг и уведомляет **вне лока**: обработчик пойдёт
обратно в пул за новым хостом и каналом, а `TAdaptiveLock` не рекурсивен.
Уведомление идёт на каждое «мёртв», а не только на переход: соединение, чей
переезд не удался, должно получить следующую попытку.

```cpp
void TCellHostPool::SetHostAlive(const TString& fqdn, bool alive)
{
    TVector<ICellHostWatcherPtr> watchers;

    with_lock (Lock) {
        auto* channel = Channels.FindPtr(fqdn);
        if (!channel) {
            return;
        }

        channel->Alive = alive;
        if (alive) {
            return;
        }

        EraseIf(
            channel->Watchers,
            [&](const auto& w)
            {
                auto watcher = w.lock();
                if (!watcher) {
                    return true;
                }
                watchers.push_back(std::move(watcher));
                return false;
            });
    }

    // outside the lock: a watcher asks the pool for another host, and this
    // lock is not recursive
    for (const auto& watcher: watchers) {
        watcher->OnHostUnavailable();
    }
}
```

Обратите внимание: снята проверка `channel->Configured`. Живость теперь
отслеживается и у хостов, узнанных на лету, — на них тоже сидят соединения.
Выбор хоста этим не затронут: `PickConfiguredHost` по-прежнему фильтрует по
`Configured && Alive`. Для `EraseIf` нужен `#include <util/generic/algorithm.h>`.

- [ ] **Step 6: Прогнать тесты**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: PASS — четыре новых теста плюс все прежние.

- [ ] **Step 7: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/host_pool.h \
        cloud/blockstore/libs/cells/impl/host_pool.cpp \
        cloud/blockstore/libs/cells/impl/host_pool_ut.cpp
git commit -m "cells: let the host pool tell connections their host is gone"
```

---

### Task 5: Пингер

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/host_pool.h`
- Modify: `cloud/blockstore/libs/cells/impl/host_pool.cpp`
- Test: `cloud/blockstore/libs/cells/impl/host_pool_ut.cpp`

**Interfaces:**
- Consumes: `ICellHostWatcher` из Task 4, конфиг из Task 3.
- Produces: ничего нового наружу — `TCellHostPool::Start()` начинает планировать
  обходы. Пул становится `std::enable_shared_from_this<TCellHostPool>`, чтобы
  запланированная задача держала его слабо.

- [ ] **Step 1: Написать падающие тесты**

Добавить **в конец** `host_pool_ut.cpp`. Дубли бутстрапа и эндпоинта уже стоят в
файле с Task 4; здесь к ним добавляется окружение с часами, его место — в
анонимном namespace после них:

```cpp
////////////////////////////////////////////////////////////////////////////////

struct TPingEnv
{
    std::shared_ptr<TTestEndpointBootstrap> EndpointsSetup =
        std::make_shared<TTestEndpointBootstrap>();
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    std::shared_ptr<TTestScheduler> Scheduler =
        std::make_shared<TTestScheduler>(TInstant::Zero());

    TBootstrap Bootstrap;

    TPingEnv()
    {
        Bootstrap.EndpointsSetup = EndpointsSetup;
        Bootstrap.Timer = Timer;
        Bootstrap.Scheduler = Scheduler;
        Bootstrap.Logging = CreateLoggingService("console");
    }

    TCellHostPoolPtr MakePool(bool migrationEnabled)
    {
        NProto::TCellConfig proto;
        proto.SetCellId("cell-1");
        proto.SetGrpcPort(9766);
        proto.SetHostMigrationEnabled(migrationEnabled);
        proto.SetHostPingPeriod(1000);
        proto.SetHostPingTimeout(500);
        proto.AddHosts()->SetFqdn("host-a");
        proto.AddHosts()->SetFqdn("host-b");
        proto.SetMinCellConnections(2);

        auto pool = std::make_shared<TCellHostPool>(
            std::make_shared<TCellConfig>(std::move(proto)),
            Bootstrap);
        pool->Start();
        return pool;
    }

    void AdvanceTime(TDuration duration)
    {
        Timer->AdvanceTime(duration);
        Scheduler->AdvanceTime(duration);
        Scheduler->RunAllScheduledTasksUntilNow();
    }
};
```

Понадобятся инклюды в начале файла:

```cpp
#include <cloud/blockstore/libs/client/multiclient_endpoint.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
```

`TTestService` уже несёт `PingHandler`: обработчики генерируются макросом
`BLOCKSTORE_SERVICE` для каждого метода сервиса, так что дублю остаётся только
его поставить.

Сами тесты:

```cpp
    Y_UNIT_TEST(ShouldPingHostsPeriodically)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.EndpointsSetup->Services["host-a"]->Pings);
    }

    Y_UNIT_TEST(ShouldMarkHostDeadWhenPingFails)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);

        env.EndpointsSetup->Services["host-a"]->PingError =
            MakeError(E_REJECTED, "host is down");

        env.AdvanceTime(TDuration::Seconds(1));

        for (ui32 i = 0; i < 10; ++i) {
            auto picked = pool->PickConfiguredHost();
            UNIT_ASSERT_C(!HasError(picked), picked.GetError());
            UNIT_ASSERT_VALUES_EQUAL("host-b", picked.GetResult().GetFqdn());
        }
    }

    Y_UNIT_TEST(ShouldReviveHostWhenPingSucceedsAgain)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);

        auto& service = env.EndpointsSetup->Services["host-a"];
        service->PingError = MakeError(E_REJECTED, "host is down");
        env.AdvanceTime(TDuration::Seconds(1));

        service->PingError = {};
        env.AdvanceTime(TDuration::Seconds(1));

        bool sawHostA = false;
        for (ui32 i = 0; i < 100; ++i) {
            auto picked = pool->PickConfiguredHost();
            sawHostA |= picked.GetResult().GetFqdn() == "host-a";
        }
        UNIT_ASSERT_C(sawHostA, "a revived host must be pickable again");
    }

    Y_UNIT_TEST(ShouldNotPingWhenMigrationIsDisabled)
    {
        TPingEnv env;
        auto pool = env.MakePool(false);

        env.AdvanceTime(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            env.EndpointsSetup->Services["host-a"]->Pings);
    }

    Y_UNIT_TEST(ShouldStopPingingWhenPoolIsGone)
    {
        TPingEnv env;
        auto pool = env.MakePool(true);

        env.AdvanceTime(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);

        pool.reset();

        env.AdvanceTime(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.EndpointsSetup->Services["host-a"]->Pings);
    }
```

- [ ] **Step 2: Прогнать и убедиться, что падает**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut -F "TCellHostPoolTest::ShouldPingHostsPeriodically"`
Expected: FAIL — пингов нет, счётчик остаётся нулём.

- [ ] **Step 3: Реализовать пингер**

В `host_pool.h` сделать пул `enable_shared_from_this` и объявить два приватных
метода:

```cpp
class TCellHostPool
    : public std::enable_shared_from_this<TCellHostPool>
{
...
private:
    ICellHostEndpointBootstrap::TGrpcEndpointBootstrapFuture
        EnsureChannelLocked(const TString& fqdn);

    void SchedulePingSweep();
    void PingSweep();
};
```

В `host_pool.cpp` — в конце `Start()`, после существующего прогрева:

```cpp
    if (Config->GetHostMigrationEnabled()) {
        SchedulePingSweep();
    }
```

и сами методы:

```cpp
void TCellHostPool::SchedulePingSweep()
{
    Bootstrap.Scheduler->Schedule(
        Bootstrap.Timer->Now() + Config->GetHostPingPeriod(),
        [weakSelf = weak_from_this()]
        {
            if (auto self = weakSelf.lock()) {
                self->PingSweep();
            }
        });
}

void TCellHostPool::PingSweep()
{
    TVector<std::pair<TString, NClient::IMultiClientEndpointPtr>> targets;

    with_lock (Lock) {
        for (const auto& [fqdn, channel]: Channels) {
            // a channel that has not finished connecting says nothing about
            // the host yet
            if (channel.Endpoint.Initialized() && channel.Endpoint.HasValue() &&
                channel.Endpoint.GetValue())
            {
                targets.emplace_back(fqdn, channel.Endpoint.GetValue());
            }
        }
    }

    auto request = std::make_shared<NProto::TPingRequest>();
    request->MutableHeaders()->SetRequestTimeout(
        Config->GetHostPingTimeout().MilliSeconds());

    for (auto& [fqdn, endpoint]: targets) {
        endpoint->Ping(MakeIntrusive<TCallContext>(), request)
            .Subscribe(
                [weakSelf = weak_from_this(), fqdn](const auto& future)
                {
                    auto self = weakSelf.lock();
                    if (!self) {
                        return;
                    }
                    self->SetHostAlive(fqdn, !HasError(future.GetValue()));
                });
    }

    SchedulePingSweep();
}
```

Инклюды в `host_pool.cpp`:

```cpp
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
```

Один и тот же `request` уходит во все пинги — запрос неизменяем с точки зрения
клиента, а заголовок в нём один на обход.

- [ ] **Step 4: Прогнать тесты**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: PASS — пять новых тестов плюс все прежние.

- [ ] **Step 5: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/host_pool.h \
        cloud/blockstore/libs/cells/impl/host_pool.cpp \
        cloud/blockstore/libs/cells/impl/host_pool_ut.cpp
git commit -m "cells: ping the hosts of a cell to track their liveness"
```

---

### Task 6: Соединение держит роутеры и привязку

Чистая перестройка внутренностей `TCellConnection`: поведение не меняется,
существующие тесты соединения должны пройти без правок. Переезд появится в
Task 7 — здесь только готовится место, куда он встанет.

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/connection.cpp`

**Interfaces:**
- Consumes: `CreateDetachableTarget` (Task 2), `IEndpointRouter` (Task 1).
- Produces: внутри `connection.cpp` появляется
  ```cpp
  struct THostBinding
  {
      TCellHostConfig HostConfig;
      IBlockStorePtr ControlService;
      ITransportSwitcherPtr Switcher;
      IDetachableTargetPtr Sink;
  };
  using THostBindingPtr = std::shared_ptr<THostBinding>;
  ```

- [ ] **Step 1: Переписать `TCellConnection` на роутеры**

Поля и конструктор:

```cpp
class TCellConnection final
    : public ICellConnection
    , public std::enable_shared_from_this<TCellConnection>
{
private:
    const TCellHostPoolPtr Pool;
    const TBootstrap Bootstrap;
    const NClient::TClientAppConfigPtr ClientConfig;
    const ICellConnectionObserverPtr Observer;
    const bool MigrationEnabled;

    // outlive any binding: this is what makes a move invisible from outside
    const IEndpointRouterPtr ControlRouter;
    const IEndpointRouterPtr DataRouter;

    TLog Log;

    mutable TAdaptiveLock Lock;
    THostBindingPtr Binding;

public:
    TCellConnection(
            TCellHostPoolPtr pool,
            TBootstrap bootstrap,
            NClient::TClientAppConfigPtr clientConfig,
            ICellConnectionObserverPtr observer,
            IEndpointRouterPtr controlRouter,
            IEndpointRouterPtr dataRouter,
            THostBindingPtr binding)
        : Pool(std::move(pool))
        , Bootstrap(std::move(bootstrap))
        , ClientConfig(std::move(clientConfig))
        , Observer(std::move(observer))
        , MigrationEnabled(binding->HostConfig.GetHostMigrationEnabled())
        , ControlRouter(std::move(controlRouter))
        , DataRouter(std::move(dataRouter))
        , Log(Bootstrap.Logging->CreateLog("BLOCKSTORE_CELLS"))
        , Binding(std::move(binding))
    {}

    ~TCellConnection() override
    {
        Pool->ReleaseControlChannel(Binding->HostConfig.GetFqdn());
    }

    TString GetHost() const override
    {
        with_lock (Lock) {
            return Binding->HostConfig.GetFqdn();
        }
    }

    IBlockStorePtr GetService() override
    {
        return std::make_shared<TControlService>(
            ControlRouter,
            shared_from_this());
    }

    IStoragePtr GetStorage() override
    {
        return CreateRemoteStorage(DataRouter, shared_from_this());
    }

    void OnMountResponse(
        const NProto::TMountVolumeResponse& response) noexcept
    {
        const auto& fqdn = response.GetTabletHost();
        if (!fqdn) {
            // the serving cell is older than this field
            return;
        }

        if (fqdn != GetHost() && Observer) {
            Observer->OnTabletHostChanged(fqdn);
        }
    }
};
```

Полей стало больше, чем нужно самому Task 6: `Bootstrap`, `ClientConfig` и
`MigrationEnabled` понадобятся переезду в Task 7, но ставятся сразу, чтобы
конструктор не переписывался дважды. `Log` нужен по той же причине — сегодня
`connection.cpp` ничего не логирует. Лок помечен `mutable`, потому что
`GetHost()` объявлен `const` в интерфейсе.

- [ ] **Step 2: Переписать сборку data-эндпоинта**

`CreateSwitchingDataEndpoint` перестаёт создавать роутер: роутер ему передают,
а возвращает он привязку. Заменить её и `SetupDataEndpoint` на:

```cpp
// Builds everything that ties a connection to one host. The data router is
// handed in rather than created: it outlives the binding, because it is the
// point at which a move swaps one host for another.
NThreading::TFuture<TResultOrError<THostBindingPtr>> SetupHostBinding(
    const TBootstrap& bootstrap,
    const TCellHostConfig& hostConfig,
    const IBlockStorePtr& controlService,
    const IEndpointRouterPtr& dataRouter)
{
    auto binding = std::make_shared<THostBinding>();
    binding->HostConfig = hostConfig;
    binding->ControlService = controlService;

    using TResult = TResultOrError<THostBindingPtr>;

    switch (hostConfig.GetTransport()) {
        case NProto::CELL_DATA_TRANSPORT_RDMA:
            if (!hostConfig.GetGrpcDataFallbackEnabled()) {
                // nothing switches here, so the rdma client has nobody to
                // report the endpoint state to
                return bootstrap.EndpointsSetup
                    ->SetupHostRdmaEndpoint(bootstrap, hostConfig)
                    .Apply(
                        [binding, dataRouter](const auto& f) -> TResult
                        {
                            const auto& result = f.GetValue();
                            if (HasError(result)) {
                                return result.GetError();
                            }
                            dataRouter->SetTarget(result.GetResult());
                            return binding;
                        });
            }

            {
                auto fallback = CreateGrpcDataEndpoint(
                    bootstrap,
                    hostConfig,
                    controlService);

                // the fallback goes in before the switcher starts: the
                // switcher may point the router at rdma straight away, and
                // installing the fallback afterwards would undo that
                dataRouter->SetTarget(fallback);

                binding->Sink = CreateDetachableTarget(dataRouter);
                binding->Switcher = StartTransportSwitching(
                    binding->Sink,
                    fallback,
                    [bootstrap, hostConfig](
                        NCloud::NStorage::NRdma::IClientEndpointHandlerPtr
                            handler)
                    {
                        return bootstrap.EndpointsSetup
                            ->SetupHostRdmaEndpoint(
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

                return MakeFuture(TResult(binding));
            }

        case NProto::CELL_DATA_TRANSPORT_GRPC:
            dataRouter->SetTarget(
                CreateGrpcDataEndpoint(bootstrap, hostConfig, controlService));
            return MakeFuture(TResult(binding));

        default:
            return MakeFuture(TResult(MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Unsupported cell data transport "
                    << NProto::ECellDataTransport_Name(
                           hostConfig.GetTransport()))));
    }
}
```

- [ ] **Step 3: Поправить `CreateCellConnection`**

Создать оба роутера до сборки привязки. Роутер требует непустую цель, поэтому
control-роутер создаётся уже с control-сервисом, а data-роутер — с ним же как
временной целью, которую `SetupHostBinding` тут же заменит настоящей:

```cpp
            auto controlService = controlEndpoint->CreateClientEndpoint(
                clientConfig->GetClientId(),
                clientConfig->GetInstanceId());

            auto controlRouter = CreateEndpointRouter(controlService);
            auto dataRouter = CreateEndpointRouter(controlService);

            return SetupHostBinding(
                       bootstrap,
                       hostConfig,
                       controlService,
                       dataRouter)
                .Apply(
                    [pool = std::move(pool),
                     bootstrap = std::move(bootstrap),
                     clientConfig = std::move(clientConfig),
                     observer = std::move(observer),
                     controlRouter = std::move(controlRouter),
                     dataRouter,
                     fqdn = std::move(fqdn)](const auto& f) mutable
                        -> TResultOrError<ICellConnectionPtr>
                    {
                        const auto& result = f.GetValue();
                        if (HasError(result)) {
                            pool->ReleaseControlChannel(fqdn);
                            return result.GetError();
                        }

                        return ICellConnectionPtr(
                            std::make_shared<TCellConnection>(
                                std::move(pool),
                                std::move(bootstrap),
                                std::move(clientConfig),
                                std::move(observer),
                                std::move(controlRouter),
                                std::move(dataRouter),
                                result.GetResult()));
                    });
```

- [ ] **Step 4: Прогнать существующие тесты**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: PASS без правок в тестах — поведение не изменилось.

- [ ] **Step 5: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/connection.cpp
git commit -m "cells: give a connection routers that outlive its host"
```

---

### Task 7: Переезд по смерти хоста

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/connection.cpp`
- Test: `cloud/blockstore/libs/cells/impl/connection_ut.cpp`

**Interfaces:**
- Consumes: `ICellHostWatcher` (Task 4), `THostBinding` и `SetupHostBinding`
  (Task 6).
- Produces: `TCellConnection` реализует `ICellHostWatcher`; приватный
  `void MigrateTo(TString fqdn, TString reason)`.

- [ ] **Step 1: Написать падающие тесты**

Тестам нужно, чтобы дубль бутстрапа отдавал **разные** сервисы для разных
хостов — иначе переезд ничем не отличить. В `connection_ut.cpp` заменить в
`TTestEnv` единственный `RdmaService` на карту по fqdn и добавить в конец файла:

```cpp
    Y_UNIT_TEST(ShouldMoveToAnotherHostWhenItsHostDies)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());

        env.Pool->SetHostAlive("host-a", false);

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        const auto requests = env.GrpcClient->Service->RequestCount;
        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(
            requests + 1,
            env.GrpcClient->Service->RequestCount);
    }

    Y_UNIT_TEST(ShouldStayWhenNoLiveHostIsLeft)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.Pool->SetHostAlive("host-b", false);
        env.Pool->SetHostAlive("host-a", false);

        // nowhere to go: dropping the connection would lose it entirely
        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldReleaseTheChannelOfTheHostItLeft)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");
        env.Pool->SetHostAlive("host-a", false);

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // the pool forgets a connection that left, so the death of the
        // abandoned host must not reach it any more
        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotLetTheOldSwitcherPullDataBack)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_RDMA, true, 0, true);

        auto connection = env.Connect("host-a");
        UNIT_ASSERT(env.EndpointsSetup->RdmaHandler);

        auto staleHandler = env.EndpointsSetup->RdmaHandler;

        env.Pool->SetHostAlive("host-a", false);
        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());

        // the abandoned host comes up after the move; its switcher must not
        // be able to point the data back at it
        staleHandler->HandleConnected();

        TTestEnv::Read(connection);
        UNIT_ASSERT_VALUES_EQUAL(0, env.RdmaServices["host-a"]->RequestCount);
    }
```

Конструктор `TTestEnv` получает четвёртым параметром
`bool hostMigrationEnabled = false`, который проставляет
`proto.SetHostMigrationEnabled(...)`, и публичное поле `Pool` уже есть.

- [ ] **Step 2: Прогнать и убедиться, что падает**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut -F "TCellConnectionTest::ShouldMoveToAnotherHostWhenItsHostDies"`
Expected: FAIL — хост остаётся `host-a`.

- [ ] **Step 3: Реализовать переезд**

`TCellConnection` начинает наследовать `ICellHostWatcher` и получает два поля
состояния переезда рядом с `Binding`:

```cpp
    bool MigrationInFlight = false;
    TString PendingTarget;
```

Остальное — `Bootstrap`, `ClientConfig`, `MigrationEnabled`, `Log` — уже
поставлено в Task 6.

Реализация:

```cpp
    void OnHostUnavailable() noexcept override
    {
        auto picked = Pool->PickConfiguredHost();
        if (HasError(picked)) {
            STORAGE_WARN(
                "[" << GetHost() << "] host is gone and there is nowhere to "
                    << "move: " << FormatError(picked.GetError()));
            return;
        }

        MigrateTo(picked.GetResult().GetFqdn(), "host is gone");
    }

private:
    void MigrateTo(TString fqdn, TString reason)
    {
        with_lock (Lock) {
            if (Binding->HostConfig.GetFqdn() == fqdn) {
                return;
            }

            if (MigrationInFlight) {
                // remembered rather than dropped: the reason that arrived
                // last is the one that still holds
                PendingTarget = std::move(fqdn);
                return;
            }

            MigrationInFlight = true;
        }

        STORAGE_INFO(
            "[" << GetHost() << "] moving to " << fqdn << ": " << reason);

        StartMigration(std::move(fqdn));
    }

    void StartMigration(TString fqdn)
    {
        auto self = shared_from_this();

        Pool->AcquireControlChannel(fqdn)
            .Subscribe(
                [self, fqdn](const auto& future) mutable
                {
                    self->OnChannelAcquired(
                        std::move(fqdn),
                        future.GetValue());
                });
    }

    void OnChannelAcquired(
        TString fqdn,
        const NClient::IMultiClientEndpointPtr& endpoint)
    {
        if (!endpoint) {
            FinishMigration(fqdn, nullptr);
            return;
        }

        auto hostConfig = Pool->MakeHostConfig(fqdn);
        auto controlService = endpoint->CreateClientEndpoint(
            ClientConfig->GetClientId(),
            ClientConfig->GetInstanceId());

        auto self = shared_from_this();
        SetupHostBinding(Bootstrap, hostConfig, controlService, DataRouter)
            .Subscribe(
                [self, fqdn](const auto& future) mutable
                {
                    const auto& result = future.GetValue();
                    self->FinishMigration(
                        fqdn,
                        HasError(result) ? nullptr : result.GetResult());
                });
    }

    void FinishMigration(const TString& fqdn, THostBindingPtr binding)
    {
        THostBindingPtr old;
        TString pending;

        with_lock (Lock) {
            if (binding) {
                old = std::move(Binding);
                Binding = binding;
            }

            MigrationInFlight = false;
            pending = std::move(PendingTarget);
            PendingTarget.clear();
        }

        if (binding) {
            // detached before anything else can reach it: the switcher of the
            // host we just left is still alive and would otherwise point the
            // data back at a host nobody talks to
            if (old->Sink) {
                old->Sink->Detach();
            }

            ControlRouter->SetTarget(binding->ControlService);

            auto self = shared_from_this();
            Pool->WatchHost(binding->HostConfig.GetFqdn(), self);
            Pool->UnwatchHost(old->HostConfig.GetFqdn(), self);
            Pool->ReleaseControlChannel(old->HostConfig.GetFqdn());
        } else {
            STORAGE_WARN(
                "[" << GetHost() << "] can't move to " << fqdn
                    << ", staying where we are");
            Pool->ReleaseControlChannel(fqdn);
        }

        if (pending) {
            MigrateTo(std::move(pending), "a move was asked for meanwhile");
        }
    }
```

Порядок в `FinishMigration` важен и разобран в спеке: `SetupHostBinding` уже
поставил новую цель data-роутеру, поэтому здесь остаётся отцепить старый sink,
перевести control-роутер и отпустить старый канал.

Деструктор не отписывается: пул держит наблюдателей слабо и выбрасывает
протухшие ссылки на ближайшем обходе.

`CreateCellConnection` подписывает соединение сразу после создания — раньше
`shared_from_this()` недоступен:

```cpp
                        auto connection = std::make_shared<TCellConnection>(...);

                        if (hostConfig.GetHostMigrationEnabled()) {
                            pool->WatchHost(fqdn, connection);
                        }

                        return ICellConnectionPtr(std::move(connection));
```

Для этого в лямбду нужно захватить ещё и `hostConfig`.

- [ ] **Step 4: Прогнать тесты**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: PASS — четыре новых теста плюс все прежние.

- [ ] **Step 5: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/connection.cpp \
        cloud/blockstore/libs/cells/impl/connection_ut.cpp
git commit -m "cells: move a connection off a host that stopped answering"
```

---

### Task 8: Переезд за тайблетом

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/connection.cpp`
- Test: `cloud/blockstore/libs/cells/impl/connection_ut.cpp`

**Interfaces:**
- Consumes: `MigrateTo` из Task 7.
- Produces: ничего нового наружу.

- [ ] **Step 1: Написать падающие тесты**

Добавить **в конец** `connection_ut.cpp`:

```cpp
    Y_UNIT_TEST(ShouldFollowTheTabletHost)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.GrpcClient->Service->TabletHostToReport = "host-b";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-b", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldFollowTheTabletHostOutsideTheConfig)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        // the tablet host need not be a configured host of the cell: the pool
        // makes a channel for it on demand
        env.GrpcClient->Service->TabletHostToReport = "host-z";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-z", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldStayWhenTheTabletHostIsTheCurrentOne)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, true);

        auto connection = env.Connect("host-a");

        env.GrpcClient->Service->TabletHostToReport = "host-a";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }

    Y_UNIT_TEST(ShouldNotFollowTheTabletHostWhenMigrationIsDisabled)
    {
        TTestEnv env(NProto::CELL_DATA_TRANSPORT_GRPC, false, 0, false);

        auto connection = env.Connect("host-a");

        env.GrpcClient->Service->TabletHostToReport = "host-b";
        connection->GetService()->MountVolume(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TMountVolumeRequest>());

        UNIT_ASSERT_VALUES_EQUAL("host-a", connection->GetHost());
    }
```

- [ ] **Step 2: Прогнать и убедиться, что падает**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut -F "TCellConnectionTest::ShouldFollowTheTabletHost"`
Expected: FAIL — соединение остаётся на `host-a`.

- [ ] **Step 3: Реализовать**

В `OnMountResponse`, рядом с уже существующим уведомлением наблюдателя:

```cpp
    void OnMountResponse(
        const NProto::TMountVolumeResponse& response) noexcept
    {
        const auto& fqdn = response.GetTabletHost();
        if (!fqdn) {
            // the serving cell is older than this field
            return;
        }

        if (fqdn == GetHost()) {
            return;
        }

        if (Observer) {
            Observer->OnTabletHostChanged(fqdn);
        }

        if (MigrationEnabled) {
            MigrateTo(fqdn, "the volume tablet lives there");
        }
    }
```

- [ ] **Step 4: Прогнать тесты**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -t -j 20 cloud/blockstore/libs/cells/impl/ut`
Expected: PASS — четыре новых теста плюс все прежние.

- [ ] **Step 5: Прогнать всё, что зависит от cells**

Run: `YA_TOKEN_PATH=/nonexistent ./ya make -tt -j 20 cloud/blockstore/libs/cells cloud/blockstore/libs/endpoints`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add cloud/blockstore/libs/cells/impl/connection.cpp \
        cloud/blockstore/libs/cells/impl/connection_ut.cpp
git commit -m "cells: follow the volume tablet to its host"
```

---
