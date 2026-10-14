# Транспорт data plane на io_uring

Дизайн замены gRPC в межхостовом data path NBS: TCP-транспорт на io_uring
с клиентом, реализующим `IBlockStore`, и сервером, диспетчеризующим в
`IBlockStore`-сервис. Тикет: NBS-7907.

## Контекст

gRPC на data path копирует payload в protobuf в обе стороны: на клиенте
`WriteBlocksLocal` делает `SgListCopy` в `TIOVector`
(`libs/client/client.cpp:122-159`), `ReadBlocksLocal` копирует ответ
обратно в sglist; на сервере gRPC разбирает `bytes` по блоку, а
`TStorageAdapter` строит sglist поверх строк. Плюс накладные самого gRPC
(HTTP/2, completion queue, per-call аллокации).

В репозитории есть:

- `cloud/storage/core/libs/io_uring` — файловый I/O через io_uring: один
  SQE на `io_uring_submit`, без сокетных операций, callback не получает
  `cqe->flags`. Для сети не подходит, не трогаем.
- `cloud/storage/core/libs/rdma` — transport-agnostic интерфейс
  `NRdma::IClient/IServer`, поверх которого написаны `client_rdma`
  (`IBlockStore` data endpoint), `service_rdma` (серверный target), cells
  (`TransportSwitcher` с gRPC-fallback), nonrepl → disk agent. Его модель
  буферов (транспорт владеет буферами фиксированного размера) навязывает
  три копии payload на пути чтения/записи и не позволяет читать данные
  сразу в целевой буфер.
- `contrib/libs/liburing` 2.8: `sendmsg_zc`, multishot recv, buf_ring,
  registered files — всё есть.
- YDB interconnect v2 (`interconnect_uring_engine`): шард-на-поток со
  своим кольцом, MPSC-очередь + eventfd для пробуждения, batched
  submit/peek, один read и один write в полёте на сокет, XDC — отдельное
  TCP-соединение для bulk-данных с приёмом `readv` прямо в выровненные
  целевые буферы. Все ошибки входа от peer'а там — `Y_ABORT`; у нас —
  разрыв.

Прототип (NBS-7907, ветка Arc недоступна, исходников нет): протокол v2
с отдельным data-соединением на том же порту, HELLO/WELCOME с
согласованием `OobMode` и лимитов, data-канал привязан одноразовым
токеном, DATA-header читается заранее, SEND_ZC с удержанием буфера до
NOTIF, direct SG I/O в обе стороны, 39 транспортных тестов на 5.15 и 6.8.
Замеры на стенде (4 КиБ, 64 блока, depth 16, NullService): gRPC 6 096
IOPS, inline 9 404, OOB 9 193, OOB+SEND_ZC с обеих сторон 10 169
(+66.8%). Inline и OOB на однородной нагрузке равны; ценность OOB — в
смешанной (мелкие RPC не стоят за большими payload). SEND_ZC даёт
+10%, но на хосте с lldpd ядро молча копирует (packet socket) — виден
только счётчик `copied_notifications`. Этот дизайн берёт из прототипа
протокол и правила владения буферами, а код пишется заново.

## Цели

- Data plane между хостами: `Ping`, `MountVolume`, `UnmountVolume`,
  `ReadBlocksLocal`, `WriteBlocksLocal`, `ZeroBlocks`. Control plane
  остаётся на gRPC.
- Ноль копий payload в user space на всём пути при `DirectSgIoEnabled`:
  клиент алиасит sglist вызывающего в iovec, сервер читает в выровненный
  буфер и отдаёт sglist поверх него, ответ уходит из буфера сервиса.
- Соединения ко многим хостам: endpoint на `(host, port)`, события
  Connected/Disconnected/Unavailable, чтобы cells переключали транспорт.
- OOB: payload отдельным TCP-соединением, прямо в целевой буфер;
  согласуемый, с inline-режимом ниже порога.
- Zero copy на уровне ядра (`SENDMSG_ZC`) — отключаемый конфигом.
- Базовое ядро 5.15; 6.x-возможности опциональны и пробуются на старте.
- Первый потребитель — cells; второй — loadtest.

## Вне рамок

- Control plane (`CreateVolume` и т.п.), unix-socket data endpoint
  vm-плагина, nonrepl → disk agent (может быть добавлен адаптером под
  `NRdma`-интерфейс позже).
- Шифрование и аутентификация: как у RDMA, доверенная внутренняя сеть.
  `Headers.Internal` сервер не трогает — так делает `rdma_target`.
- Multishot recv, provided buffers, registered files/buffers, SQPOLL,
  несколько data-сокетов на endpoint, автоотключение ZC по
  `copied_notifications`. Всё это — эксперименты после замеров v1.
- Собственные PING-кадры: живость — `SO_KEEPALIVE` + `TCP_USER_TIMEOUT`,
  зависший, но живой сервер даёт `E_TIMEOUT` на запросы, как у RDMA.

## Размещение кода

```
cloud/blockstore/libs/uring/
  transport/        generic движок, без зависимостей от blockstore-сервиса
    public.h        forward declarations, EOobMode, ESendZeroCopyMode
    client.h        IClient, IClientEndpoint, IClientEndpointHandler,
                    IClientRequestHandler, TClientRequest
    server.h        IServer, IServerEndpoint, IServerHandler
    config.h/.cpp   TClientConfig/TServerConfig из протобуфов
    protocol.h      структуры кадров, константы, версии
    connection.*    TConnection: main/data сокеты, парсер, очереди
    ring.*          TRing: кольцо + поток + MPSC + eventfd + timerfd
    client.cpp      TClient, TClientEndpoint, reconnect
    server.cpp      TServer, TSession, accept, токены
    ut/
  config.h/.cpp     TUringConfig (обёртка над NProto::TUringConfig)
  protocol.h/.cpp   TBlockStoreProtocol: msgId + TProtoMessageSerializer
  client.h/.cpp     CreateUringDataEndpoint, CreateUringEndpointClient
  server.h/.cpp     CreateBlockstoreServerUringTarget
  ut/
cloud/storage/core/protos/uring.proto     TUringClient, TUringServer
cloud/blockstore/config/uring.proto       TUringConfig
```

`cloud/storage/core/libs/io_uring` — другая библиотека (файловый I/O),
общая у них только `contrib/libs/liburing`. Движок в `transport/` не
знает про `IBlockStore`; если понадобится filestore — переедет в
`cloud/storage/core/libs` целиком.

Пространство имён транспорта — `NCloud::NBlockStore::NUring`; интерфейс
намеренно повторяет `NRdma` там, где от него зависят потребители
(события endpoint'а, `CancelRequest`, `Stop()` → future,
`TryForceReconnect`), но это отдельные типы: формально io_uring к RDMA
отношения не имеет. Общий слой можно вынести потом.

## Транспортный интерфейс

```cpp
struct TClientRequest
{
    IClientRequestHandlerPtr Handler;
    std::unique_ptr<TNullContext> Context;

    TString Proto;             // заголовок запроса, копируется транспортом
    TGuardedSgList SendData;   // данные запроса; алиасятся в iovec
    TGuardedSgList RecvData;   // куда читать данные ответа
    TDuration Timeout;         // 0 → MaxResponseDelay из конфига
};

struct IClientRequestHandler
{
    // Вызывается на IO-потоке. status == 0: proto — ответ, dataBytes —
    // сколько байт легло в RecvData. status != 0: proto — сериализованный
    // TError. proto валиден только во время вызова.
    virtual void HandleResponse(
        TClientRequestPtr req, ui32 status, TStringBuf proto,
        size_t dataBytes) = 0;
};

struct IClientEndpointHandler
{
    virtual void HandleConnected() = 0;
    virtual void HandleDisconnected() = 0;
    virtual void HandleUnavailable() = 0;
};

struct IClientEndpoint
{
    // Из любого потока. Возвращает id для CancelRequest.
    virtual ui64 SendRequest(TClientRequestPtr, TCallContextBasePtr) = 0;
    virtual void CancelRequest(ui64 id) = 0;
    // Резолвится после teardown на IO-потоке; ждать с IO-потока нельзя.
    virtual NThreading::TFuture<void> Stop() = 0;
    virtual void TryForceReconnect() = 0;
};

struct IClient: IStartable
{
    // Endpoint отдаётся до подключения; handler сообщает состояние.
    virtual TResultOrError<IClientEndpointPtr> StartEndpoint(
        TString host, ui32 port, IClientEndpointHandlerPtr) = 0;
    virtual void DumpHtml(IOutputStream&) const = 0;
};

struct IServerHandler
{
    virtual TCallContextBasePtr CreateCallContext() = 0;
    // На IO-потоке. data — в буфере транспорта, выровненном на 4 КиБ,
    // живёт до SendResponse/SendError. proto валиден только во время
    // вызова. ctx валиден до SendResponse/SendError независимо от судьбы
    // сессии.
    virtual void HandleRequest(
        void* ctx, TCallContextBasePtr, TStringBuf proto,
        TSgList data) = 0;
};

struct IServerEndpoint
{
    // Из любого потока. data алиасится, owner удерживается до завершения
    // записи (включая NOTIF) или до смерти сессии.
    virtual void SendResponse(
        void* ctx, TStringBuf proto, TSgList data,
        std::shared_ptr<void> owner) = 0;
    virtual void SendError(void* ctx, ui32 error, TStringBuf message) = 0;
};

struct IServer: IStartable
{
    virtual IServerEndpointPtr StartEndpoint(
        TString host, ui32 port, IServerHandlerPtr) = 0;
    virtual void DumpHtml(IOutputStream&) const = 0;
};
```

Отличие от `NRdma` одно: транспорт не владеет данными. Запрос несёт
sglist'ы вызывающего, ответ сервера — sglist сервиса с владельцем.
`Proto` для транспорта непрозрачен; blockstore-слой кладёт туда
`TProtoMessageSerializer::Serialize(msgId, proto)` с `DataLen = 0`, как
RDMA-слой.

Коды статуса ответа клиенту: `0` — успех; иначе код ошибки, `proto` —
`TError`. Транспорт сам выставляет `E_REJECTED` (нет соединения,
превышен лимит, разрыв), `E_TIMEOUT`, `E_CANCELLED`, `E_ARGUMENT`
(размер данных ответа не совпал с `RecvData`).

## Протокол

Все поля little-endian в порядке хоста, как в RDMA-протоколе. В каждом
кадре `Magic`, чтобы рассинхрон ловился сразу.

### Соединение

Пара TCP-сокетов на один порт транспорта: *main* (заголовки, proto,
inline-данные, ошибки) и *data* (только payload). Клиент открывает
main, шлёт HELLO, получает WELCOME; если согласован OOB — открывает
data, шлёт DATA_HELLO с токеном из WELCOME, получает DATA_WELCOME.
Только после этого endpoint Connected.

```
HELLO        main, клиент → сервер                      24 байта
  ui32 Magic; ui16 Version; ui8 Role = MAIN; ui8 OobMode;
  ui32 MaxProtoSize; ui32 MaxDataSize; ui32 MaxInflightBytes; ui32 Pad;

WELCOME      main, сервер → клиент                      32 байта
  ui32 Magic; ui16 Version; ui8 Status; ui8 OobMode;
  ui32 MaxProtoSize; ui32 MaxDataSize; ui32 MaxInflightBytes;
  ui64 DataToken; ui32 Pad;

DATA_HELLO   data, клиент → сервер                      16 байт
  ui32 Magic; ui16 Version; ui8 Role = DATA; ui8 Pad; ui64 DataToken;

DATA_WELCOME data, сервер → клиент                      8 байт
  ui32 Magic; ui16 Version; ui8 Status; ui8 Pad;
```

Согласование `OobMode` (`OFF | AUTO | REQUIRED` с каждой стороны):
`OFF` против `REQUIRED` — `Status = CONFIG_MISMATCH`, разрыв; `OFF`
против чего угодно — OOB выключен; иначе включён. Лимиты
`MaxProtoSize`/`MaxDataSize` — минимум из двух; `MaxInflightBytes` в
WELCOME — серверный лимит на сессию, клиент применяет минимум из него и
своего, чтобы отказывать локально, не тратя round trip. Версия: клиент шлёт свою, сервер принимает свою и предыдущую;
иначе `VERSION_MISMATCH` — клиент логирует и не ускоряет reconnect.

`DataToken` — случайные 64 бита, живёт в серверной map до привязки или
`DataTokenTtl` (10 с), одноразовый. Неверный токен — сокет закрывается.

### Кадры на main

```
TFrameHeader                                             24 байта
  ui32 Magic; ui8 Type; ui8 Flags; ui16 Pad;
  ui64 RequestId; ui32 ProtoLen; ui32 DataLen;
[ProtoLen байт proto][DataLen байт data, если Flags & INLINE]

Type:  REQUEST | RESPONSE | ERROR
Flags: INLINE  — данные следуют за proto в этом кадре
       OOB     — данные придут data-кадром с тем же RequestId
```

`ERROR` — ответ транспорта или handler'а: `proto` = сериализованный
`TError`, `DataLen = 0`. `RequestId` — 64-битный монотонный счётчик на
endpoint, ответы в любом порядке.

Выбор пути данных отправителем: OOB согласован и
`DataLen >= OobThreshold` → `OOB`, иначе `INLINE`. `DataLen = 0` —
флагов нет.

### Data-канал

```
TDataHeader                                              16 байт
  ui32 Magic; ui32 DataLen; ui64 RequestId;
[DataLen байт]
```

Data-кадры идут в том же порядке, что их main-кадры. Приёмник читает
`TDataHeader` сразу после предыдущего payload, но payload читает только
когда main-кадр с этим `RequestId` допущен (лимиты проверены,
назначение известно). Несовпадение `RequestId` с головой FIFO или
`DataLen` с main-кадром — ошибка протокола. Если назначения нет (запрос
отменён, guard не взялся, сервер отказал) — payload вычитывается в
scratch-буфер соединения и отбрасывается; поток остаётся согласованным.

Отправителю ограничений на взаимный порядок сокетов не нужно: data
может уйти раньше main-кадра, приёмник просто не читает его до допуска.

### Ошибки протокола

Любая — разрыв соединения с логом, никогда не `Y_ABORT`: плохой
`Magic`, неизвестный `Type`, `ProtoLen > MaxProtoSize`,
`DataLen > MaxDataSize`, рассинхрон data-канала, неверный токен, второй
DATA_HELLO. Ответ на неизвестный `RequestId` (запрос уже завершён по
таймауту или отмене) ошибкой не является: inline-ответ игнорируется,
OOB-payload вычитывается в scratch — его размер известен из обоих
заголовков.

## Движок

### Потоки и кольца

`TClient` и `TServer` владеют `IoThreads` потоками (`URING.CLI<i>` /
`URING.SRV<i>`), по кольцу на поток (`QueueSize` SQE). Флаги кольца:
пробуем `SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN` (6.1+), при
`EINVAL` — без флагов. Соединение прибивается к потоку при создании
(round-robin) и всё его состояние трогается только с этого потока.

Цикл потока:

1. вычитать MPSC-очередь команд (`TLockFreeList`, как `InputRequests`
   у RDMA): новые запросы, ответы сервера, отмены, команды сессий;
2. построить SQE для соединений, у которых есть работа;
3. `io_uring_submit_and_wait(1)`;
4. `io_uring_peek_batch_cqe` по 64, диспетчеризация по `user_data`;
5. повторить.

Пробуждение извне: eventfd с постоянно взведённым `read` на кольце.
Producer пишет в eventfd, только если поток пометил себя спящим
(`WaitingForCq`); поток ставит флаг, перепроверяет очередь и лишь потом
ждёт. Таймеры — periodic timerfd (100 мс) на кольце: дедлайны
запросов, backoff reconnect'а, TTL токенов.

`user_data` — указатель на `TIoOp` из пула соединения:
`{Connection*, Kind, iovec[64], IovCount, Offset, Request*}`. Op живёт
до последнего ожидаемого CQE (`SENDMSG_ZC` — до `F_NOTIF`);
соединение не освобождается, пока счётчик op'ов не ноль. Частичные
send/recv — сдвиг `Offset` по iovec, тот же op отправляется снова.

### Соединение

`TConnection{Main, Data}` общий для клиента и сервера.

Main, приём: один `recv` в полёте в приёмный буфер соединения (64 КиБ,
растёт до `MaxProtoSize + OobThreshold + header`); парсер разбирает
полные кадры, хвост переносится в начало. Inline-данные копируются из
буфера в назначение под коротким guard'ом.

Main, отправка: один `sendmsg` в полёте с батчем из очереди кадров: до
64 iovec и `SendWindowBytes` (1 МиБ). Заголовок и proto — `TString`
кадра, inline-payload — iovec'и из sglist вызывающего.

Data, приём — автомат: `ReadingHeader` (16 байт) → ждём допуска кадра с
этим `RequestId` в голове FIFO → `ReadingPayload` (`recvmsg` в
назначение, до 64 iovec, цикл по частичным) → снова `ReadingHeader`.

Data, отправка: FIFO data-кадров, по одному op в полёте: header из
`TString` кадра + payload iovec'и. `SENDMSG_ZC`, если согласно режиму
и порогу (см. zero copy).

Сокеты: `TCP_NODELAY`, `SO_KEEPALIVE` (`KeepAliveIdle`/`Interval`/
`Count`), `TCP_USER_TIMEOUT`. Сокеты блокирующие: с `O_NONBLOCK`
io_uring возвращает `-EAGAIN` вместо ожидания (урок YDB).

### Запрос на клиенте

1. `SendRequest` с любого потока: лимиты по атомикам
   (`MaxInflightRequests`, `MaxInflightBytes` — `Proto + SendData +
   RecvData`) → при превышении сразу `E_REJECTED`; выдать `RequestId`;
   в MPSC; разбудить.
2. На IO-потоке: endpoint не Connected → `E_REJECTED`. Иначе взять
   write-guard (`SendData.Acquire()`; не взялся → `E_CANCELLED`),
   сериализовать кадр, поставить в очередь main (и data при OOB),
   записать в `ActiveRequests[id]` с дедлайном.
3. Send CQE (для ZC — NOTIF): write-guard отпущен. Ответа backend'а не
   ждём.
4. `ERROR` → завершить со статусом. `RESPONSE + INLINE` → короткий
   read-guard, копия в `RecvData` (размер должен совпасть), завершить.
   `RESPONSE + OOB` → `AwaitingData`; когда `TDataHeader` с этим id в
   голове FIFO: `DataLen == размер RecvData` → read-guard → `recvmsg` в
   `RecvData` → отпустить guard → завершить. Guard не взялся или размер
   не тот → payload в scratch, завершить `E_CANCELLED`/`E_ARGUMENT`.
5. `HandleResponse` вызывается после того, как все guard'ы запроса
   отпущены — всегда, включая ошибку, таймаут и отмену. Durable-клиент
   на retry закрывает предыдущий depender sglist'а
   (`durable.cpp:431`), и это не должно блокироваться.

Таймаут и отмена — один механизм: запрос помечается; если ни один SQE
не ссылается на его память — завершается сейчас (`E_TIMEOUT` /
`E_CANCELLED`), поздний ответ отправится в scratch; если ссылается —
завершится, когда этот SQE закончится. Разрыва соединения по таймауту
нет: зависший peer убивает `TCP_USER_TIMEOUT`.

### Запрос на сервере

1. Поток 0 держит one-shot `accept` и перевзводит его. Новый сокет:
   опции, прочитать первые 24 байта. HELLO → `TSession`, прибить к
   потоку (round-robin), согласовать, WELCOME, токен в map.
   DATA_HELLO → найти сессию по токену → отдать fd на её поток
   командой; DATA_WELCOME. Всё до привязки — на потоке 0, это не
   горячий путь.
2. `REQUEST`: `ProtoLen`/`DataLen` в лимитах, иначе разрыв;
   `InflightBytes + ProtoLen + DataLen > MaxInflightBytes` или
   `InflightRequests >= MaxInflightRequests` → `ERROR(E_REJECTED)`,
   inline-данные пропущены, OOB-данные в scratch. Иначе
   `TServerRequest{RequestId, Proto копия, TAlignedBuffer(DataLen,
   4 КиБ)}`: inline — копия, OOB — `recvmsg` в буфер при допуске.
   Затем `HandleRequest` на IO-потоке.
3. `SendResponse`/`SendError` с любого потока → команда на поток
   сессии → кадр (inline/OOB по порогу) → после send CQE/NOTIF
   отпустить `owner`, освободить контекст, списать inflight. Сессии уже
   нет → отпустить `owner`, освободить контекст.

### Reconnect

Разрешение имени — на отдельном потоке `URING.RESOLVE` (блокирующий
`getaddrinfo` не должен стоять на IO-потоке), результат — командой на
IO-поток; `connect` через кольцо; handshake; `HandleConnected`. Разрыв:
`HandleDisconnected`, незавершённые запросы → `E_REJECTED`, backoff
`TBackoffDelayProvider` от 100 мс до `MaxReconnectDelay`; на максимуме —
`HandleUnavailable` (как `Hanging()` у RDMA); `TryForceReconnect`
сбрасывает backoff и переподключает сразу. Reconnect не начинается,
пока teardown предыдущего соединения не завершён.

### Teardown

Обе стороны, одинаково:

1. состояние `Closing`, новые запросы → `E_REJECTED`;
2. `shutdown(SHUT_RDWR)` на обоих сокетах;
3. `io_uring_prep_cancel` на каждый op в полёте, submit;
4. дождаться всех CQE, включая NOTIF (счётчик op'ов → 0): до этого
   страницы могут быть у TCP-стека, освобождать память нельзя;
5. `close` с `SO_LINGER{1, 0}` (RST) — иначе при большой записи peer'а
   страницы ZC висят, а fd переиспользуется при reconnect;
6. отпустить guard'ы и `owner`'ы, завершить запросы, событие.

`IClientEndpoint::Stop()` — та же последовательность на IO-потоке,
future резолвится в конце. `IClient::Stop()` останавливает все
endpoint'ы и join'ит потоки. Деструктор endpoint'а не ждёт (урок
cells: ждать с потока, который и должен завершить stop, — deadlock).

## Zero copy

Два независимых переключателя, оба — на старте, сквозные для inline и
OOB:

**`DirectSgIoEnabled`** — zero copy в user space. `true`: sglist
вызывающего алиасится в iovec (отправка) и является назначением
`recvmsg` (приём). `false` — staging: на отправке payload копируется в
`TString` транспорта под коротким guard'ом, на приёме читается в буфер
транспорта и копируется в `RecvData` под коротким guard'ом. Это две
ветки с `SgListCopy`, остальной путь общий. Зачем в проде: прототип не
подтвердил выигрыш direct SG по CPU — нужны чередующиеся прогоны на
одном бинарнике; и откат конфигом, если direct SG удерживает guard
дольше, чем ждёт какой-то потребитель.

**`SendZeroCopyMode: OFF | AUTO | REQUIRED`** + `SendZeroCopyThreshold`
— zero copy в ядре (`IORING_OP_SENDMSG_ZC`, 6.0+). Это локальная
способность отправителя, на проводе не видна, согласовывать с peer'ом
нечего. `AUTO`: `io_uring_get_probe` на старте; opcode есть —
используем, нет — одно предупреждение, сенсор
`SendZeroCopySupported = 0`, обычный `sendmsg`. `REQUIRED`: без opcode
— ошибка старта. ZC применяется к одной операции отправки, если её
payload не меньше порога (64 КиБ: ниже этого пиннинг страниц и второй
CQE дороже memcpy). Буфер (и `TString` кадра) удерживается до
`F_NOTIF`; `F_MORE` на первом CQE означает, что NOTIF ещё будет.
Hidden copy (loopback, lldpd packet socket, NIC без SG) виден только по
`SendZcCopiedNotifications` — в v1 это метрика, не автоотключение.

Правила guard'ов при `DirectSgIoEnabled`:

- write-guard берётся перед постановкой SQE, отпускается после send
  CQE (NOTIF при ZC), не дожидаясь ответа backend'а;
- read-guard берётся после проверки metadata ответа (статус, размер),
  отпускается после recv CQE;
- оба отпускаются до завершения future, включая ошибку, таймаут,
  отмену;
- до 64 iovec на операцию, длинный sglist режется на несколько op;
  нулевые спаны пропускаются; при ошибке посреди приёма в `RecvData`
  могут остаться частичные данные — запрос завершается ошибкой.

## Blockstore-слой

`protocol.h`: `TBlockStoreProtocol` с теми же msgId, что
`client_rdma/protocol.h` (Read/Write/Zero/Ping/Mount/Unmount × Request/
Response), и `TProtoMessageSerializer` с зарегистрированными proto.

`client.h`:

```cpp
struct TUringEndpointConfig { TString Address; ui32 Port; };

// Для cells: endpoint отдаётся до подключения, handler сообщает состояние.
TResultOrError<IBlockStorePtr> CreateUringDataEndpoint(
    ILoggingServicePtr, NUring::IClientPtr, ITraceSerializerPtr,
    ITaskQueuePtr, const TUringEndpointConfig&,
    NUring::IClientEndpointHandlerPtr);

// Для loadtest: данные через uring, Mount/Unmount через volumeClient.
IBlockStorePtr CreateUringEndpointClient(
    ILoggingServicePtr, NUring::IClientPtr, IBlockStorePtr volumeClient,
    ITraceSerializerPtr, ITaskQueuePtr, const TUringEndpointConfig&);
```

`TUringDataEndpoint: TBlockStoreImpl` реализует `Ping`, `MountVolume`,
`UnmountVolume`, `ReadBlocksLocal`, `WriteBlocksLocal`, `ZeroBlocks`;
остальное — `E_NOT_IMPLEMENTED`, как у `TRdmaDataEndpoint`.
`WriteBlocksLocal`: `SendData = request->Sglist`, без
`TakeDataOwnership`. `ReadBlocksLocal`: `RecvData = request->Sglist`.
`Timeout` — из `Headers.RequestTimeout`. Ответ: `Serializer()->Parse`,
throttling и trace как в RDMA. Промис ставится на IO-потоке; cells
перекладывают продолжения в свой `TaskQueue`.

`server.h`: `CreateBlockstoreServerUringTarget(config, logging,
traceSerializer, NUring::IServerPtr, IBlockStorePtr service)`, пул
`URING` на `WorkerThreads`. `HandleRequest` → `TaskQueue` → по msgId:
`WriteBlocks` — `TGuardedSgList` поверх `data` → `WriteBlocksLocal`;
`ReadBlocks` — `TGuardedBuffer` на `BlockSize * BlocksCount` →
`ReadBlocksLocal` → `SendResponse(ctx, proto, sglist буфера,
owner = буфер)`; `Zero`/`Ping`/`Mount`/`Unmount` — proto-only. Ответ
больше `MaxProtoSize` — `ClearTrace`, как в `rdma_target`.

## Интеграция

**Конфиг.** `cloud/storage/core/protos/uring.proto`:

```
TUringClient  IoThreads=2, QueueSize=4096, MaxInflightRequests=1024,
              MaxInflightBytes=64 MiB, MaxProtoSize=64 KiB,
              MaxDataSize=8 MiB, SendWindowBytes=1 MiB,
              OobMode=AUTO, OobThreshold=128 KiB,
              SendZeroCopyMode=AUTO, SendZeroCopyThreshold=64 KiB,
              DirectSgIoEnabled=true, MaxResponseDelay=30 s,
              MaxReconnectDelay=30 s, TcpUserTimeout=30 s,
              KeepAliveIdle=5 s, KeepAliveInterval=5 s, KeepAliveCount=3
TUringServer  то же минус reconnect/MaxResponseDelay, плюс Backlog=128,
              DataTokenTtl=10 s; MaxInflight* — на сессию
```

`cloud/blockstore/config/uring.proto`: `TUringConfig {ClientEnabled,
Client, ServerEnabled, Server, BlockstoreServerTargetEnabled,
BlockstoreServerTarget{Endpoint, WorkerThreads}}` — зеркало
`TRdmaConfig`. Daemon: `--uring-file`, `InitUringConfig`,
`InitUringClient`/`InitUringServer`/`UringTarget` рядом с RDMA-аналогами
(`daemon/common/bootstrap.cpp:271-283, 869, 1066-1069`), фабрики
`UringClientFactory`/`UringServerFactory` в `daemon/ydb/bootstrap.h` по
образцу `RdmaClientFactory`.

**Cells.** `CELL_DATA_TRANSPORT_URING = 4`, `UringPort` в
`cells.proto`. `TBootstrap` получает `UringClient`; `RdmaTaskQueue`
переименовывается в `DataTaskQueue` и обслуживает оба.
`ICellHostEndpointBootstrap::SetupHostUringEndpoint`. В
`BuildHostBinding`/`PrepareDataPath`/`InstallBinding` ветка RDMA
обобщается до «предпочтительный транспорт с gRPC-fallback»
(`rdmaOnly` → `preferredOnly`); `GrpcDataFallbackEnabled` и
`RdmaSettleTime` действуют для обоих. `TransportSwitcher` типизирован
на `NRdma::IClientEndpointHandlerPtr` — в cells добавляется адаптер
`NUring::IClientEndpointHandler → NRdma::IClientEndpointHandler` из трёх
форвардящих методов; switcher не меняется.

**Loadtest.** `IPC_URING = 6` в `EClientIpcType` (`mount.proto`);
`UringHost`/`UringPort` в `TClientConfig`; `CreateAndStartUringClient`
и ветка `IPC_URING` в `CreateEndpointDataClient` — копия RDMA-ветки
(`bootstrap.cpp:479-491`): control через gRPC `CreateEndpoint()`,
данные через uring; `NeedSocketPath`.

**Мониторинг.** `DumpHtml` клиента и сервера: endpoint'ы/сессии,
состояние, inflight, счётчики. Сенсоры под `uring/client` и
`uring/server`: `Requests`, `Errors`, `Timeouts`, `Rejected`,
`Reconnects`, `DirectSendBytes`, `DirectRecvBytes`, `StagingCopyBytes`,
`InlineBytes`, `OobBytes`, `SqePerSubmit`, `CqePerBatch`,
`SendZcRequests`, `SendZcNotifications`, `SendZcCopiedNotifications`,
`SendZcConfirmedBytes`, `SendZcPending`, `SendZeroCopySupported`.

## Ошибки

| Ситуация | Действие |
|---|---|
| Нет соединения при `SendRequest` | `E_REJECTED` сразу |
| Лимит inflight на клиенте | `E_REJECTED` сразу, без отправки |
| Лимит inflight на сервере | `ERROR(E_REJECTED)`, payload отброшен |
| `ProtoLen`/`DataLen` вне лимита | разрыв соединения |
| Рассинхрон data-канала, плохой `Magic`, неверный токен | разрыв |
| Ответ на неизвестный id | игнорируется; OOB-payload в scratch |
| Дедлайн запроса | `E_TIMEOUT`, см. «Таймаут и отмена» |
| `CancelRequest` | `E_CANCELLED`, тот же механизм |
| Guard `SendData` не взялся | `E_CANCELLED` до отправки |
| Guard `RecvData` не взялся / размер не тот | payload в scratch, `E_CANCELLED` / `E_ARGUMENT` |
| Разрыв с запросами в полёте | все → `E_REJECTED` после teardown |
| Handler сервера бросил исключение | `SendError(E_REJECTED)`, как в `rdma_target` |
| `SendZeroCopyMode = REQUIRED` без opcode | ошибка старта |
| `OobMode` несовместим | `CONFIG_MISMATCH`, reconnect по backoff, `HandleUnavailable` на максимуме |
| Версия несовместима | то же |

## Тестирование

`transport/ut` — реальные сокеты на loopback, клиент и сервер в одном
процессе, эфемерный порт:

- протокол: кодирование/разбор кадров, парсер на побайтовой подаче,
  матрица HELLO/WELCOME (`OobMode` × обе стороны, версия, лимиты),
  неверный токен, DATA-header раньше main-кадра;
- данные: proto-only, inline, OOB, вокруг `OobThreshold`, sglist > 64
  iovec, нулевые спаны, частичные send/recv (маленький `SO_SNDBUF`),
  `DirectSgIoEnabled` on/off — одинаковый результат;
- лимиты/ошибки: `MaxInflight*` → `E_REJECTED`; `DataLen > MaxDataSize`
  → разрыв и reconnect; серверный reject с OOB-payload; таймаут
  (handler не отвечает) → `E_TIMEOUT`, `Close()` на sglist после
  завершения не блокируется; `CancelRequest`; разрыв с запросами в
  полёте; `Stop()` с inflight; события endpoint'а, backoff,
  `TryForceReconnect`;
- SEND_ZC: `AUTO` и `OFF` на loopback, `REQUIRED` без opcode — ошибка
  старта; тесты, требующие ZC, пропускаются, если probe его не нашёл;
- многопоточный `SendRequest`; сборка под TSAN.

`libs/uring/ut` — endpoint + target поверх loopback с тестовым
`IBlockStore` по образцу `server_test.h`: round trip шести методов,
целостность данных, ошибки сервиса, throttling/trace; durable-клиент
сверху — retry после `E_TIMEOUT` не блокируется.

Cells-ut: ветка `URING` в выборе транспорта, адаптер handler'а,
fallback-переключение с фейковым `NUring::IClient`. Loadtest:
`cloud/blockstore/tests/loadtest` с `IPC_URING` против локального nbs с
`BlockstoreServerTargetEnabled`, смешанные запросы с проверкой данных.

**Бенчмарк** — методика тикета, без отдельного микробенча: loadtest
против `--service null` с `AddReadResponseData`, 4 КиБ × 64 блока,
depth 16, 60 с; A/B на одном бинарнике по `DirectSgIoEnabled`,
`SendZeroCopyMode`, `OobMode`/`OobThreshold`; снимаем IOPS, латентности,
CPU процесса и ring-потока, `SqePerSubmit`/`CqePerBatch`,
ZC-счётчики. Ориентир — не хуже прототипа: +60% IOPS к gRPC на том же
стенде.

## Этапы

1. `cloud/blockstore/libs/uring/transport` — интерфейсы, протокол,
   движок, ut. Без зависимостей от blockstore-сервиса.
2. `cloud/blockstore/libs/uring` — конфиг, протокол, endpoint, target,
   ut; `--uring-file` и daemon wiring; loadtest `IPC_URING`.
3. Cells: enum и порт, bootstrap, адаптер handler'а, обобщение
   fallback-логики, тесты.
4. Замеры на стенде; по результатам — multishot recv / buf_ring /
   SQPOLL, автоотключение ZC по `copied_notifications`, несколько
   data-сокетов на endpoint.

Раскатка: сервер с включённым target раньше клиентов; transport для
ячейки переключается конфигом cells, с `GrpcDataFallbackEnabled` на
время перехода.

## Риски

- Head-of-line на data-канале: payload 4 МиБ задерживает данные
  последующих запросов того же endpoint'а. В cells соединение — на
  started endpoint (на том), так что в пределах одного тома; несколько
  data-сокетов — после замеров.
- SEND_ZC может молча копировать (lldpd packet socket, loopback): есть
  метрика, нет автоотключения; CPU на уведомления при этом тратится.
- Один IO-поток на соединение: endpoint с очень высокой нагрузкой
  упирается в его ядро; `IoThreads` распределяет только между
  endpoint'ами.
- `TCP_USER_TIMEOUT` как единственный детектор зависшего peer'а: живой,
  но не отвечающий сервер виден только по `E_TIMEOUT`; cells решают
  по своим health-check'ам, как с RDMA.
- 5.15 без `DEFER_TASKRUN`: больше пробуждений ring-потока; замеры
  прототипа на 6.6 этого не покрывают.
