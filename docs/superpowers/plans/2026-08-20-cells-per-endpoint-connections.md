# Cells: пер-эндпоинтные соединения и проброс хоста таблетки — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Соединение с данными чужой ячейки принадлежит одному поднятому эндпоинту и умирает вместе с ним; ответ на монтирование сообщает хост, где живёт таблетка.

**Architecture:** `libs/cells` выдаёт владеющий `ICellConnection` вместо значения `TCellHostEndpoint`; состояние хостов съезжает в `TCellHostPool`, конечный автомат `TCellHost` удаляется. Параллельно `TabletHost` пробрасывается от таблетки тома через сервис в публичный `TMountVolumeResponse` и оседает в подсказочном кэше пула. Переезд между хостами в этот инкремент не входит.

**Tech Stack:** C++20, Arcadia-сборка (`ya make`), протобуф, `library/cpp/testing/unittest`, актор-система YDB.

**Spec:** `docs/superpowers/specs/2026-08-20-cells-per-endpoint-connections-design.md`

## Global Constraints

- Сборка и тесты только через `ya`, с обходом токена: `YA_TOKEN_PATH=/nonexistent ./ya make ...` из корня репозитория. Без этого бутстрап падает с HTTP 400.
- Размер сьюта определяет флаг: `-t` — только small, `-tt` — плюс medium. `cloud/blockstore/libs/storage/service/ut` подключает `medium.inc`, поэтому там нужен `-tt`; с `-t` сьют молча пропускается и прогон рапортует `Total 0 tests` и `Ok`, что выглядит как успех. Сьюты `libs/cells` — small, им хватает `-t`.
- Новые тесты добавлять **в конец** сьюта, а не рядом с тематически близким.
- Публичные proto — `proto3`, номера полей только новые, существующие не переиспользовать.
- Внутренние proto `libs/storage/protos` — тоже `proto3`.
- Новые поля обязаны деградировать: отсутствие `TabletHost` в ответе означает «информации нет», а не ошибку.
- Переезд между хостами (`SwitchSession`, `OnConnectionLost`) в этот план **не входит**. Если задача тянет за собой переезд — остановиться и сообщить.
- Ничего не коммитить в `main`; работа идёт в отдельной ветке.

---

## File Structure

**Проброс `TabletHost`:**

- `cloud/blockstore/libs/storage/protos/volume.proto` — поле в `TAddClientResponse`.
- `cloud/blockstore/libs/storage/volume/volume_actor_addclient.cpp` — таблетка заполняет поле.
- `cloud/blockstore/libs/storage/service/service_events_private.h` — поле в `TMountRequestProcessed`.
- `cloud/blockstore/libs/storage/service/service_state.h` — поле в `TVolumeInfo`.
- `cloud/blockstore/libs/storage/service/volume_session_actor_mount.cpp` — перенос значения по цепочке.
- `cloud/blockstore/public/api/protos/mount.proto` — поле в `TMountVolumeResponse`.

**Переделка соединений:**

- `cloud/blockstore/libs/cells/iface/connection.h` / `.cpp` — новый `ICellConnection`, `ICellConnectionObserver`.
- `cloud/blockstore/libs/cells/iface/cell_manager.h` — `GetCellEndpoint` заменяется на `CreateConnection`.
- `cloud/blockstore/libs/cells/impl/host_pool.h` / `.cpp` — новый `TCellHostPool`.
- `cloud/blockstore/libs/cells/impl/connection.h` / `.cpp` — новый `TCellConnection`.
- `cloud/blockstore/libs/cells/impl/cell_impl.*`, `cell_host*.*` — удаление автомата.
- `cloud/blockstore/libs/endpoints/session_manager.cpp` — хранение хэндла в `TEndpoint`.

---

### Task 1: `TabletHost` доезжает до клиента

Одна задача на всю цепочку: по отдельности звенья не наблюдаемы, а тест есть только сквозной.

**Files:**
- Modify: `cloud/blockstore/libs/storage/protos/volume.proto` (после строки 129, `VolumeClientMigrationInProgress = 8`)
- Modify: `cloud/blockstore/public/api/protos/mount.proto` (`TMountVolumeResponse`, после `ServiceVersionInfo = 5`)
- Modify: `cloud/blockstore/libs/storage/volume/volume_actor_addclient.cpp:31` (`CreateAddClientResponse`)
- Modify: `cloud/blockstore/libs/storage/service/service_events_private.h:118-152` (`TMountRequestProcessed`)
- Modify: `cloud/blockstore/libs/storage/service/service_state.h` (`TVolumeInfo`)
- Modify: `cloud/blockstore/libs/storage/service/volume_session_actor_mount.cpp:104` (`CreateInternalMountResponse`) и `:933` (`HandleVolumeAddClientResponse`)
- Test: `cloud/blockstore/libs/storage/service/service_ut_mount.cpp`

**Interfaces:**
- Produces: `NProto::TMountVolumeResponse::GetTabletHost()` — FQDN узла, на котором исполняется таблетка тома; пустая строка, если сервер старый или значение неизвестно. Потребляется Task 6.

- [ ] **Step 1: Написать падающий тест**

В `service_ut_mount.cpp`, в `Y_UNIT_TEST_SUITE(TServiceMountVolumeTest)`:

```cpp
    Y_UNIT_TEST(ShouldReportTabletHostInMountResponse)
    {
        TTestEnv env;
        ui32 nodeIdx = SetupTestEnv(env);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateVolume();
        service.AssignVolume();

        auto response = service.MountVolume();
        UNIT_ASSERT_C(!HasError(response->GetError()), response->GetError());
        UNIT_ASSERT_VALUES_EQUAL(
            FQDNHostName(),
            response->Record.GetTabletHost());
    }
```

Если `FQDNHostName` не виден, добавить `#include <util/system/hostname.h>`.

- [ ] **Step 2: Убедиться, что тест падает**

```
YA_TOKEN_PATH=/nonexistent ./ya make -tt cloud/blockstore/libs/storage/service \
    -F "TServiceMountVolumeTest::ShouldReportTabletHostInMountResponse"
```

Ожидание: ошибка компиляции `no member named 'GetTabletHost'`.

- [ ] **Step 3: Добавить поля в оба proto**

`libs/storage/protos/volume.proto`, в `TAddClientResponse` после поля 8:

```proto
    // Host the volume tablet is running on.
    string TabletHost = 9;
```

`public/api/protos/mount.proto`, в `TMountVolumeResponse` после поля 5:

```proto
    // The host where the volume tablet is currently running. Empty if the
    // serving side does not report it.
    string TabletHost = 6;
```

- [ ] **Step 4: Таблетка заполняет поле**

В `volume_actor_addclient.cpp`, в `CreateAddClientResponse` (строка 31), после создания `response`:

```cpp
    response->Record.SetTabletHost(FQDNHostName());
```

Проверить, что `#include <util/system/hostname.h>` есть; если нет — добавить. Образец того же приёма: `volume_actor_statvolume.cpp:296`.

- [ ] **Step 5: Протащить значение через сервис**

`service_events_private.h`, в `TMountRequestProcessed` — новое поле после `VolumeSessionRestartRequired` и параметр конструктора последним:

```cpp
        const bool VolumeSessionRestartRequired;
        const TString TabletHost;
```

```cpp
                bool volumeSessionRestartRequired,
                TString tabletHost)
            : ...
            , VolumeSessionRestartRequired(volumeSessionRestartRequired)
            , TabletHost(std::move(tabletHost))
        {}
```

`service_state.h`, в `TVolumeInfo` рядом с `TabletId`:

```cpp
    TString TabletHost;
```

`volume_session_actor_mount.cpp`: в `TMountRequestActor` добавить член `TString TabletHost;`, в `HandleVolumeAddClientResponse` (строка 933) сразу после `Volume = msg->Record.GetVolume();`:

```cpp
    TabletHost = msg->Record.GetTabletHost();
```

Передать `TabletHost` в конструктор `TMountRequestProcessed` (найти его вызов в этом же файле), а в обработчике `TMountRequestProcessed` в `TVolumeSessionActor` записать `VolumeInfo->TabletHost = msg->TabletHost;`.

В `CreateInternalMountResponse` (строка 104), внутри `if (!HasError(error))`:

```cpp
        response->Record.SetTabletHost(volumeInfo.TabletHost);
```

- [ ] **Step 6: Убедиться, что тест проходит**

```
YA_TOKEN_PATH=/nonexistent ./ya make -tt cloud/blockstore/libs/storage/service \
    -F "TServiceMountVolumeTest::ShouldReportTabletHostInMountResponse"
```

Ожидание: PASS.

- [ ] **Step 7: Прогнать сьюты целиком**

```
YA_TOKEN_PATH=/nonexistent ./ya make -tt \
    cloud/blockstore/libs/storage/service \
    cloud/blockstore/libs/storage/volume
```

Ожидание: все GOOD. `TabletHost` пока никто не читает, регрессий быть не должно.

- [ ] **Step 8: Коммит**

```bash
git add cloud/blockstore/libs/storage/protos/volume.proto \
        cloud/blockstore/public/api/protos/mount.proto \
        cloud/blockstore/libs/storage/volume/volume_actor_addclient.cpp \
        cloud/blockstore/libs/storage/service/service_events_private.h \
        cloud/blockstore/libs/storage/service/service_state.h \
        cloud/blockstore/libs/storage/service/volume_session_actor_mount.cpp \
        cloud/blockstore/libs/storage/service/service_ut_mount.cpp
git commit -m "[Blockstore] report volume tablet host in MountVolume response"
```

---

### Task 2: `TCellHostPool` — каналы и здоровье хостов

**Files:**
- Create: `cloud/blockstore/libs/cells/impl/host_pool.h`, `host_pool.cpp`
- Modify: `cloud/blockstore/libs/cells/impl/ya.make`, `cloud/blockstore/libs/cells/impl/ut/ya.make`
- Test: `cloud/blockstore/libs/cells/impl/host_pool_ut.cpp`

**Interfaces:**
- Consumes: `TCellConfig`, `TCellHostConfig`, `TBootstrap`, `ICellHostEndpointBootstrap` — существующие.
- Produces:
  ```cpp
  class TCellHostPool {
  public:
      TCellHostPool(TCellConfigPtr config, TBootstrap bootstrap);

      TResultOrError<TCellHostConfig> PickConfiguredHost() const;
      TCellHostConfig MakeHostConfig(const TString& fqdn) const;

      NThreading::TFuture<TResultOrError<NClient::IMultiClientEndpointPtr>>
          AcquireControlChannel(const TString& fqdn);
      void ReleaseControlChannel(const TString& fqdn);

      void SetHostAlive(const TString& fqdn, bool alive);

      void RememberTabletHost(const TString& diskId, const TString& fqdn);
      TString GetTabletHostHint(const TString& diskId) const;
  };
  using TCellHostPoolPtr = std::shared_ptr<TCellHostPool>;
  ```
  Потребляется Task 3 и Task 6.

- [ ] **Step 1: Написать падающие тесты**

`host_pool_ut.cpp`:

```cpp
#include "host_pool.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

namespace {

TCellConfigPtr MakeConfig()
{
    NProto::TCellConfig proto;
    proto.SetCellId("cell-1");
    proto.SetGrpcPort(9766);
    proto.SetRdmaPort(10020);
    proto.SetMinCellConnections(1);
    proto.AddHosts()->SetFqdn("host-a");
    proto.AddHosts()->SetFqdn("host-b");
    return std::make_shared<TCellConfig>(std::move(proto));
}

}   // namespace

Y_UNIT_TEST_SUITE(TCellHostPoolTest)
{
    Y_UNIT_TEST(ShouldMakeHostConfigForUnlistedFqdn)
    {
        TCellHostPool pool(MakeConfig(), TBootstrap{});

        auto host = pool.MakeHostConfig("host-z");
        UNIT_ASSERT_VALUES_EQUAL("host-z", host.GetFqdn());
        UNIT_ASSERT_VALUES_EQUAL(9766, host.GetGrpcPort());
        UNIT_ASSERT_VALUES_EQUAL(10020, host.GetRdmaPort());
    }

    Y_UNIT_TEST(ShouldNotPickDeadHost)
    {
        TCellHostPool pool(MakeConfig(), TBootstrap{});

        pool.SetHostAlive("host-a", false);
        pool.SetHostAlive("host-b", false);
        UNIT_ASSERT(HasError(pool.PickConfiguredHost()));

        pool.SetHostAlive("host-b", true);
        auto picked = pool.PickConfiguredHost();
        UNIT_ASSERT_C(!HasError(picked), picked.GetError());
        UNIT_ASSERT_VALUES_EQUAL("host-b", picked.GetResult().GetFqdn());
    }

    Y_UNIT_TEST(ShouldKeepTabletHostHint)
    {
        TCellHostPool pool(MakeConfig(), TBootstrap{});

        UNIT_ASSERT_VALUES_EQUAL("", pool.GetTabletHostHint("vol-1"));

        pool.RememberTabletHost("vol-1", "host-z");
        UNIT_ASSERT_VALUES_EQUAL("host-z", pool.GetTabletHostHint("vol-1"));

        pool.RememberTabletHost("vol-1", "host-y");
        UNIT_ASSERT_VALUES_EQUAL("host-y", pool.GetTabletHostHint("vol-1"));
    }
}

}   // namespace NCloud::NBlockStore::NCells
```

- [ ] **Step 2: Убедиться, что тесты падают**

Добавить `host_pool_ut.cpp` в `SRCS` файла `impl/ut/ya.make`, затем:

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells/impl
```

Ожидание: `host_pool.h` не найден.

- [ ] **Step 3: Реализовать пул**

`host_pool.h` — объявления по блоку Interfaces выше. Внутреннее состояние:

```cpp
private:
    const TCellConfigPtr Config;
    const TBootstrap Bootstrap;

    struct TChannel
    {
        NClient::IMultiClientEndpointPtr Endpoint;
        ui32 RefCount = 0;
        bool Configured = false;
        bool Alive = true;
    };

    mutable TAdaptiveLock Lock;
    THashMap<TString, TChannel> Channels;
    THashMap<TString, TString> TabletHostByDiskId;
```

Правила: конфигурационные хосты заводятся в `Channels` в конструкторе с `Configured = true`; `ReleaseControlChannel` уменьшает `RefCount` и выбрасывает запись только если `!Configured && RefCount == 0`; `PickConfiguredHost` выбирает случайный среди `Configured && Alive` и возвращает `MakeError(E_REJECTED, "No live hosts in cell " + CellId)`, если таких нет; `MakeHostConfig` собирает `TCellHostConfig` из `NProto::TCellHostConfig` с заданным `Fqdn` и текущего `*Config`.

Добавить `host_pool.cpp` в `SRCS` файла `impl/ya.make`.

- [ ] **Step 4: Убедиться, что тесты проходят**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells/impl
```

Ожидание: три теста `TCellHostPoolTest` — GOOD.

- [ ] **Step 5: Коммит**

```bash
git add cloud/blockstore/libs/cells/impl/host_pool.h \
        cloud/blockstore/libs/cells/impl/host_pool.cpp \
        cloud/blockstore/libs/cells/impl/host_pool_ut.cpp \
        cloud/blockstore/libs/cells/impl/ya.make \
        cloud/blockstore/libs/cells/impl/ut/ya.make
git commit -m "[Blockstore] introduce TCellHostPool for cells"
```

---

### Task 3: `ICellConnection` — владеющее соединение

**Files:**
- Create: `cloud/blockstore/libs/cells/iface/connection.h`
- Create: `cloud/blockstore/libs/cells/impl/connection.h`, `connection.cpp`
- Modify: `cloud/blockstore/libs/cells/iface/ya.make`, `impl/ya.make`, `impl/ut/ya.make`
- Test: `cloud/blockstore/libs/cells/impl/connection_ut.cpp`

**Interfaces:**
- Consumes: `TCellHostPool` из Task 2.
- Produces:
  ```cpp
  struct ICellConnectionObserver
  {
      virtual ~ICellConnectionObserver() = default;
      virtual void OnTabletHostChanged(TString fqdn) noexcept = 0;
  };
  using ICellConnectionObserverPtr = std::shared_ptr<ICellConnectionObserver>;

  struct ICellConnection
  {
      virtual ~ICellConnection() = default;
      virtual TString GetHost() const = 0;
      virtual IBlockStorePtr GetService() const = 0;
      virtual IStoragePtr GetStorage() const = 0;
  };
  using ICellConnectionPtr = std::shared_ptr<ICellConnection>;
  ```
  Потребляется Task 4 и Task 5.

`OnConnectionLost` в этот инкремент не входит — он нужен только переезду.

- [ ] **Step 1: Написать падающие тесты**

`connection_ut.cpp`:

```cpp
#include "connection.h"
#include "host_pool.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

namespace {

struct TTestObserver final: public ICellConnectionObserver
{
    TVector<TString> Reported;

    void OnTabletHostChanged(TString fqdn) noexcept override
    {
        Reported.push_back(std::move(fqdn));
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TCellConnectionTest)
{
    Y_UNIT_TEST(ShouldReportOnlyForeignTabletHost)
    {
        auto observer = std::make_shared<TTestObserver>();
        auto conn = CreateTestConnection("host-a", observer);

        conn->OnMountResponse("");         // старый сервер
        conn->OnMountResponse("host-a");   // тот же хост
        UNIT_ASSERT_VALUES_EQUAL(0, observer->Reported.size());

        conn->OnMountResponse("host-z");
        UNIT_ASSERT_VALUES_EQUAL(1, observer->Reported.size());
        UNIT_ASSERT_VALUES_EQUAL("host-z", observer->Reported[0]);
    }
}

}   // namespace NCloud::NBlockStore::NCells
```

`CreateTestConnection` — хелпер в `connection.h` под `#ifdef`-ом не нужен; объявить в `connection.h` (impl) обычную фабрику `CreateCellConnection(...)` и в тесте собрать её с фейковым `ICellHostEndpointBootstrap`, как это уже делается в `cell_host_impl_ut.cpp`. `OnMountResponse(fqdn)` — публичный метод конкретного `TCellConnection`, вызываемый обёрткой control-сервиса.

- [ ] **Step 2: Убедиться, что тест падает**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells/impl
```

Ожидание: `connection.h` не найден.

- [ ] **Step 3: Реализовать соединение**

`TCellConnection` хранит `TString Host`, `ICellConnectionObserverPtr Observer`, `TCellHostPoolPtr Pool`, control-эндпоинт из пула и собственный data-эндпоинт. `GetService()` возвращает обёртку, которая на успешном `MountVolume` зовёт `OnMountResponse(response.GetTabletHost())`; `OnMountResponse` дёргает наблюдателя только при непустом и отличном от `Host` значении, а также зовёт `Pool->RememberTabletHost(diskId, fqdn)`.

**Владение:** объекты, возвращаемые `GetService()`/`GetStorage()`, держат `shared_ptr` на `TCellConnection`. Деструктор `TCellConnection` зовёт `Pool->ReleaseControlChannel(Host)` и закрывает data-эндпоинт.

- [ ] **Step 4: Убедиться, что тест проходит**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells/impl
```

Ожидание: `TCellConnectionTest::ShouldReportOnlyForeignTabletHost` — GOOD.

- [ ] **Step 5: Коммит**

```bash
git add cloud/blockstore/libs/cells/iface/connection.h \
        cloud/blockstore/libs/cells/impl/connection.h \
        cloud/blockstore/libs/cells/impl/connection.cpp \
        cloud/blockstore/libs/cells/impl/connection_ut.cpp \
        cloud/blockstore/libs/cells/iface/ya.make \
        cloud/blockstore/libs/cells/impl/ya.make \
        cloud/blockstore/libs/cells/impl/ut/ya.make
git commit -m "[Blockstore] introduce owning ICellConnection for cells"
```

---

### Task 4: `ICellManager::CreateConnection` вместо `GetCellEndpoint`

**Files:**
- Modify: `cloud/blockstore/libs/cells/iface/cell_manager.h`, `cell_manager.cpp` (стаб)
- Modify: `cloud/blockstore/libs/cells/impl/cell_manager_impl.h`, `cell_manager_impl.cpp`
- Test: `cloud/blockstore/libs/cells/impl/cell_manager_ut.cpp`

**Interfaces:**
- Produces:
  ```cpp
  TResultOrError<ICellConnectionPtr> CreateConnection(
      const TString& cellId,
      const TString& fqdn,               // пусто -> живой конфигурационный хост
      const NClient::TClientAppConfigPtr& clientConfig,
      ICellConnectionObserverPtr observer);
  ```
  Потребляется Task 5.

- [ ] **Step 1: Написать падающий тест**

В `cell_manager_ut.cpp`:

```cpp
    Y_UNIT_TEST(ShouldFailConnectionForUnknownCell)
    {
        auto manager = CreateTestCellManager();

        auto result = manager->CreateConnection(
            "no-such-cell",
            {},
            CreateTestClientConfig(),
            nullptr);

        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_NOT_FOUND, result.GetError().GetCode());
    }
```

Здесь же важное поведенческое изменение: сегодня `GetCellEndpoint` на неизвестной ячейке **бросает** через `Y_ENSURE`, хотя возвращает `TResultOrError`. Новый метод обязан возвращать ошибку.

- [ ] **Step 2: Убедиться, что тест падает**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells/impl \
    -F "TCellManagerTest::ShouldFailConnectionForUnknownCell"
```

- [ ] **Step 3: Заменить метод**

Удалить `GetCellEndpoint` из `ICellManager` и из стаба `TCellManagerStub`, добавить `CreateConnection`. В `TCellManager` держать `THashMap<TString, TCellHostPoolPtr> Pools` вместо `THashMap<TString, ICellPtr> Cells`; `CreateConnection` находит пул, при пустом `fqdn` берёт `PickConfiguredHost()`, иначе `MakeHostConfig(fqdn)`, и создаёт `TCellConnection`.

Стаб возвращает `MakeError(E_NOT_IMPLEMENTED, "not implemented")`.

- [ ] **Step 4: Убедиться, что тесты проходят**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells
```

- [ ] **Step 5: Коммит**

```bash
git add cloud/blockstore/libs/cells
git commit -m "[Blockstore] replace GetCellEndpoint with owning CreateConnection"
```

---

### Task 5: `session_manager` держит соединение

**Files:**
- Modify: `cloud/blockstore/libs/endpoints/session_manager.cpp` (`TEndpoint`, `CreateStorageDataClient:861`, `CreateEndpoint:899`)
- Test: `cloud/blockstore/libs/endpoints/session_manager_ut.cpp`

**Interfaces:**
- Consumes: `ICellManager::CreateConnection` из Task 4.
- Produces: `TEndpoint` владеет `ICellConnectionPtr`; соединение закрывается при `RemoveSession`.

- [ ] **Step 1: Написать падающий тест**

В `session_manager_ut.cpp` — фейковый `ICellManager`, считающий живые соединения:

```cpp
    Y_UNIT_TEST(ShouldReleaseCellConnectionOnStopEndpoint)
    {
        auto cellManager = std::make_shared<TTestCellManager>();
        auto env = CreateTestEnv(cellManager);

        env.StartEndpoint(DefaultDiskId, "cell-1");
        UNIT_ASSERT_VALUES_EQUAL(1, cellManager->AliveConnections());

        env.StopEndpoint(DefaultDiskId);
        UNIT_ASSERT_VALUES_EQUAL(0, cellManager->AliveConnections());
    }
```

`TTestCellManager::AliveConnections()` считает выданные `ICellConnection`, у которых ещё не отработал деструктор.

- [ ] **Step 2: Убедиться, что тест падает**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/endpoints \
    -F "TSessionManagerTest::ShouldReleaseCellConnectionOnStopEndpoint"
```

- [ ] **Step 3: Перевести `CreateStorageDataClient` на соединение**

Вместо `CellManager->GetCellEndpoint(cellId, clientConfig)`:

```cpp
    auto result = CellManager->CreateConnection(
        cellId,
        /* fqdn */ {},
        clientConfig,
        shared_from_this());
    if (HasError(result)) {
        return result.GetError();
    }

    auto connection = result.ExtractResult();
    service = connection->GetService();
    storage = connection->GetStorage();
```

Вернуть `connection` наверх вместе с клиентом и положить в `TEndpoint`. `TSessionManager` реализует `ICellConnectionObserver`; в этом инкременте `OnTabletHostChanged` только логирует и инкрементирует счётчик — переезда нет.

- [ ] **Step 4: Убедиться, что тест проходит**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/endpoints
```

- [ ] **Step 5: Коммит**

```bash
git add cloud/blockstore/libs/endpoints
git commit -m "[Blockstore] tie cell connection lifetime to endpoint"
```

---

### Task 6: Кэш подсказок питает выбор стартового хоста

**Files:**
- Modify: `cloud/blockstore/libs/cells/impl/cell_manager_impl.cpp` (`CreateConnection`)
- Test: `cloud/blockstore/libs/cells/impl/cell_manager_ut.cpp`

**Interfaces:**
- Consumes: `TCellHostPool::GetTabletHostHint`, `RememberTabletHost` из Task 2.

- [ ] **Step 1: Написать падающий тест**

```cpp
    Y_UNIT_TEST(ShouldUseTabletHostHintForNewConnection)
    {
        auto manager = CreateTestCellManager();

        auto first = manager->CreateConnection(
            "cell-1", {}, CreateTestClientConfig(), nullptr);
        UNIT_ASSERT_C(!HasError(first), first.GetError());

        ReportTabletHost(manager, "cell-1", DefaultDiskId, "host-z");

        auto second = manager->CreateConnectionForDisk(
            "cell-1", DefaultDiskId, CreateTestClientConfig(), nullptr);
        UNIT_ASSERT_C(!HasError(second), second.GetError());
        UNIT_ASSERT_VALUES_EQUAL(
            "host-z",
            second.GetResult()->GetHost());
    }
```

Кэш ключуется диском, поэтому `CreateConnection` нужен `diskId`. Добавить его параметром, а не заводить второй метод: у всех вызовов он под рукой.

- [ ] **Step 2: Убедиться, что тест падает**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells/impl \
    -F "TCellManagerTest::ShouldUseTabletHostHintForNewConnection"
```

- [ ] **Step 3: Использовать подсказку**

В `CreateConnection`, когда `fqdn` пуст:

```cpp
    auto fqdnToUse = fqdn;
    if (!fqdnToUse) {
        fqdnToUse = pool->GetTabletHostHint(diskId);
    }
```

Если подсказка есть — `MakeHostConfig(fqdnToUse)`; если подключение к подсказанному хосту не удалось, откатиться на `PickConfiguredHost()`. Промах кэша не должен приводить к отказу старта эндпоинта.

- [ ] **Step 4: Убедиться, что тесты проходят**

```
YA_TOKEN_PATH=/nonexistent ./ya make -t cloud/blockstore/libs/cells
```

- [ ] **Step 5: Коммит**

```bash
git add cloud/blockstore/libs/cells
git commit -m "[Blockstore] pick cell host by tablet host hint"
```

---

### Task 7: Удаление автомата хостов

Выполняется последней: пока `ICellHost` есть, предыдущие задачи могут на него опираться, и удаление в конце делает диф читаемым.

**Files:**
- Delete: `cloud/blockstore/libs/cells/impl/cell_host.h`, `cell_host.cpp`, `cell_host_impl.h`, `cell_host_impl.cpp`, `cell_host_impl_ut.cpp`
- Delete: `cloud/blockstore/libs/cells/impl/cell.h`, `cell.cpp`, `cell_impl.h`, `cell_impl.cpp`, `cell_impl_ut.cpp`
- Modify: `cloud/blockstore/libs/cells/iface/config.h` (убрать `GetStrictCellIdCheckInDescribeVolume`)
- Modify: `impl/ya.make`, `impl/ut/ya.make`

- [ ] **Step 1: Убедиться, что ссылок не осталось**

```
grep -rn "ICellHost\|CreateHost\|ICellPtr\|CreateCell\b" cloud/blockstore/
grep -rn "GetStrictCellIdCheckInDescribeVolume" cloud/
```

Ожидание: пусто, кроме удаляемых файлов. Если что-то осталось — доработать вызывающую сторону, прежде чем удалять.

- [ ] **Step 2: Удалить файлы и почистить `ya.make`**

- [ ] **Step 3: Прогнать всё**

```
YA_TOKEN_PATH=/nonexistent ./ya make -tt \
    cloud/blockstore/libs/cells \
    cloud/blockstore/libs/endpoints \
    cloud/blockstore/libs/storage/service
YA_TOKEN_PATH=/nonexistent ./ya make cloud/blockstore/apps/server
```

Ожидание: тесты GOOD, `nbsd` собирается.

- [ ] **Step 4: Коммит**

```bash
git add -A cloud/blockstore/libs/cells
git commit -m "[Blockstore] drop cell host state machine"
```

---

## Self-Review

**Покрытие спеки (инкремент 1).** `ICellConnection` с RAII — Task 3 и 5. `TCellHostPool` с двумя популяциями и health — Task 2. Снос `ICellHost` — Task 7. `TabletHost` в ответе — Task 1. Кэш подсказок — Task 2 (хранение) и Task 6 (использование). Метрика переездов не нужна: переездов в этом инкременте нет, в Task 5 остаётся счётчик уведомлений.

Не покрыто намеренно, перенесено в инкремент 2: `OnConnectionLost`, `SwitchEndpointToCellHost`, откат при частичном отказе, гонки с релокацией, страница «Cells».

**Типы.** `ICellConnectionPtr`, `TCellHostPoolPtr`, `GetTabletHostHint`/`RememberTabletHost`, `CreateConnection(cellId, fqdn, diskId, clientConfig, observer)` — в Task 6 к сигнатуре добавляется `diskId`; Task 4 и Task 5 должны быть приведены к финальной сигнатуре при выполнении Task 6.

**Оставшаяся неточность, которую исполнителю придётся уточнить по месту:** в Task 1, Step 5 не указан номер строки вызова конструктора `TMountRequestProcessed` и обработчика этого события в `TVolumeSessionActor` — оба находятся в `volume_session_actor_mount.cpp`, ищутся по имени типа.
