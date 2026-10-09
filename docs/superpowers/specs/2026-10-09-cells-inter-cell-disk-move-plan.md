# Переезд диска между ячейками: план

Статус: план, не дизайн. Написан 2026-10-09 по результатам разбора кода,
исходные условия согласованы с владельцем. Ссылки на строки — на main на
момент написания.

## Исходные условия

- Переезд диска из ячейки A в ячейку B **внутри одной зоны**. Результат:
  копия в B становится основным диском, ВМ переключается на неё, источник в A
  удаляется.
- Инициатор — задача миграции Disk Manager по **приватному API DM**
  (`PrivateService`, `internal/api/private_service.proto`): Compute про
  ячейки не знает и к этой операции доступа не имеет. Копирование и
  переключение делает NBS тем же механизмом, что при переезде внутри ячейки
  (linked volumes).
- Сегодняшняя DM-ветка миграции между ячейками (копирование по чекпоинтам
  через gRPC с заморозкой источника) остаётся под флагом как запасной путь, в
  норме не используется.
- Авторизация межъячеечных запросов пока не рассматривается: новые запросы
  между ячейками идут по доверенному каналу без неё, как сегодня
  `Mount`/`Unmount`/`Describe`.
- Данные на follower идут обычными `WriteBlocks`/`ZeroBlocks` от
  `copy-volume-client` **без маунта**, по data-каналу соединения (RDMA с gRPC
  fallback). Таблетка такие записи уже принимает; мешает только проверка
  сессии в storage service, которую межъячеечный форвард обходит.

## Что есть сейчас

### Внутри ячейки: linked volumes (leader → follower)

Описано в `doc/blockstore/storage/changing-media-type.md`:

1. Создаётся `<id>-copy` с тегом `source-disk-id`, затем публичный
   `CreateVolumeLink` (`public/api/grpc/service.proto:87`).
2. Сервис шлёт `TEvLinkLeaderVolumeToFollowerRequest` в локальный volume proxy
   (`service_actor_create_volume_link.cpp:61-71`). Поля
   `LeaderShardId`/`FollowerShardId` в протоколе и localdb есть, но **нигде не
   заполняются** и используются только в `GetDirectCopyUsage` как строковое
   сравнение (`follower_disk_actor.cpp:26-35`).
3. Таблетка лидера описывает оба диска через локальный SSProxy
   (`create_volume_link_actor.cpp:42-55`) и пропагирует link на follower через
   volume proxy (`propagate_to_follower.cpp:47-72`).
4. Над партицией лидера встаёт `TFollowerDiskActor`
   (`volume_actor_startstop.cpp:408-462`): фоновая миграция диапазонов плюс
   зеркалирование записей. Назначение — `TVolumeAsPartitionActor`: подменяет
   `DiskId`, ставит `ClientId = copy-volume-client`, шлёт Write/Zero в
   локальный volume proxy (`volume_as_partition_actor.cpp:58-83, 298-299`).
5. Follower принимает записи только от copy-клиентов
   (`volume_actor_forward.cpp:730-816`); `copy-volume-client` — немаунченный
   in-process клиент, допустимый, пока других клиентов нет (825-840).
6. По окончании follower → `Leader`, лидер → `LeadershipTransferred`. Клиент на
   remount получает `PrincipalDiskId` и переключает сессию
   (`switchable_client.cpp:284-307` → `session_manager.cpp:1127-1205`).
7. Principal удаляет старого лидера через локальный storage service
   (`volume_actor_follower.cpp:239-249`).

### Между ячейками: DM `MigrateDisk` с `is_migration_between_cells`

`services/disks/migrate_disk_task.go`, `service.go:802-868`. Для DM ячейка —
ключ в конфиге `Zones` со своими endpoint'ами, тесты гоняют `zone-a` →
`zone-a-shard1`. Шаги: `Clone` копии в B с тегом `source-disk-id` →
`dataplane.ReplicateDisk` (MountRO в A, MountRW с fill generation в B, копия по
чекпоинтам) → `Freeze` источника тегом `read-only` → последняя итерация →
`Finishing`: удаление источника, снятие тега, `DiskRelocated`.

Тома друг о друге не знают, живые записи не зеркалируются, на последнюю
итерацию есть простой. Этот путь уходит под флаг.

### Что уже умеет `libs/cells`

- `ICellManager::DescribeVolume` находит ячейку диска; диск с тегом
  `source-disk-id` → `MigrationDestination`, игнорируется
  (`describe_volume.cpp:328-344`).
- `CreateConnection(cellId)`: control (`GetService`: Mount/Unmount/Describe со
  штампом `CellId`) и data (`GetStorage`: `TRemoteStorage` поверх примаунченной
  сессии).
- `TCellForwardService` пропускает без авторизации только
  `{Describe, Mount, Unmount}` с `CellId` по secure-порту
  (`forward_service.cpp:18-31`).
- Слой сессий уже умеет переключиться на principal в **другой** ячейке:
  `SwitchSessionForEndpoint` описывает новый диск через `CellManager`
  (`session_manager.cpp:1143-1158`).
- `libs/cells` доступен только из `libs/daemon` и `libs/endpoints`; акторы
  томов в `libs/storage` до него не достают.

## Целевая схема

```
DM migrate_disk_task (между ячейками)
  1. Clone: копия <id>-copy в B с тегом source-disk-id      (есть)
  2. CreateVolumeLink в A: follower = <id>-copy, cell = B   (новое: FollowerShardId)
  3. ждать getlinkstatus в A == LeadershipTransferred       (новое в DM)
  4. удалить источник в A, DiskRelocated                    (есть)

NBS, таблетка лидера в A
  - describe follower в B                 через мост в cells
  - пропагация link на follower в B       через мост (новый UpdateVolumeLink)
  - данные на follower в B                через мост (data-канал, без маунта)
  - передача лидерства                    как сейчас
  - снятие тега source-disk-id            новое: при передаче лидерства

Клиент (endpoint)
  - remount → PrincipalDiskId → SwitchSession → describe через cells → B   (есть)
```

## Распределение работ

### DM

Сегодняшний `Clone` для linked volumes не годится в трёх местах, и это надо
решить раньше всего остального:

- **ID копии.** `Clone` создаёт копию в B **с тем же ID**
  (`multi_zone_client.go:138`), а `CreateVolumeLink` отвергает равные ID
  (`service_actor_create_volume_link.cpp:150`). Linked volumes требуют
  `<id>-copy`. **Решено:** внешний ID не меняется, переименование в NBS не
  делаем. Операция приватная, Compute её не вызывает и продолжает знать диск
  под `id`, поэтому смена ID — внутреннее дело DM: в записи диска
  (`resources/disks.go:255`) появляется постоянное поле **физического ID**
  (`<id>-copy` после переезда), и DM подставляет его во все вызовы NBS по
  этому диску. Это alias в DM, не межсервисный протокол. Что затрагивает:
  все места в DM, где внешний `id` уходит в NBS как `DiskId` (mount, describe,
  snapshot, delete, placement group — `Describe` отдаёт физические ID, `Alter`
  принимает логические, `placementgroup/service.go:248`) — их надо провести
  через alias в обе стороны; `DiskRelocated` (`resources/disks.go:1088`)
  меняет ячейку и физический ID одной транзакцией после рубежа; до рубежа
  alias не меняется, так что отмена отката не требует.

  **Alias в DM не покрывает endpoint'ы.** Node стартует endpoint по
  логическому `id`, который ему даёт Compute при attach
  (`compute-api/.../disks_attach.go:351` → `node/.../endpoints.go:149`), DM в
  этом пути не участвует, и `StartEndpointRequest` с этим `id` сохраняется
  на Node для рестарта. Пока A жив, его таблетка редиректит на `<id>-copy`
  через `PrincipalDiskId`; **после удаления A** новый или перезапущенный
  endpoint с `DiskId = id` не найдёт ничего: SSProxy при describe пробует
  `<id>-copy` (`ss_proxy_actor_describevolume.cpp:197-201`), но
  cells-describe (`describe_volume.cpp`) этого фоллбэка **не делает** —
  только фильтр по тегу. Выбор `id`/`<id>-copy` в SSProxy идёт **по
  существованию, не по роли**: `<id>-copy` возвращается только когда пути
  `id` нет (`ss_proxy_actor_describevolume.cpp:189-204`); пока A жив,
  describe по `id` находит A, а о копии клиент узнаёт лишь из
  `PrincipalDiskId` в ответе на mount (`service_actor_describe.cpp:140`
  передаёт `""`). Две разные ситуации, два разных средства в NBS (этап 2):
  - **A жив, лидерство передано** — `DescribeVolumeResponse` от A заполняет
    `Volume.PrincipalDiskId` (поле есть, `volume.proto:289`, на describe не
    заполняется) и новое поле `PrincipalCellId` (ячейка follower'а из
    `FollowerShardId` link'а). `SwitchSessionForEndpoint` идёт к B по ним
    напрямую, не завися от того, снят ли уже тег с B. Полезно и внутри
    ячейки: клиент узнаёт о переключении на describe, не дожидаясь remount.
  - **A удалён** — фоллбэк в cells-describe: если `id` не найден **ни в
    одной** ячейке и не задан `ExactDiskIdMatch`, повторить поиск для
    `<id>-copy` (как SSProxy). Для живого A не срабатывает никогда.
    Три оговорки, без которых фоллбэк опасен:
    1. первый проход по ячейкам обязан идти с `ExactDiskIdMatch`, иначе
       SSProxy каждой ячейки сам подставит `<id>-copy` и cells-describe
       возьмёт первый успешный ответ (`TrySetValue`,
       `describe_volume.cpp:231`) без проверки, что это именно копия
       этого диска;
    2. найденный `<id>-copy` принимается только если у него есть (или был)
       link на `id` — иначе это может быть **независимый** диск с таким
       именем (DM такие ID не запрещает); проверка — по
       `TLeaderDiskInfo` на follower'е, которую describe B должен отдавать;
    3. повторный переезд не наращивает суффикс: физическое имя
       **чередуется** `id → <id>-copy → id`. Это не новое правило, а то, как
       NBS уже устроен: `GetNextDiskId` (`volume_label.cpp:134-138`) делает
       ровно это, а все места с `GetLogicalDiskId` (статистика,
       `service_actor_destroy.cpp:403`, `session_manager.cpp:1006`) считают
       `id` и `<id>-copy` одним логическим диском — третье имя туда не
       вписалось бы. Безопасно, потому что источник удаляется до конца
       переезда (шаг 6), и его имя снова свободно; DM обязан не начинать
       следующий переезд того же диска, пока удаление источника не
       подтверждено (per-disk owner, шаг 8). Alias в DM хранит текущее
       физическое имя из двух.
       **Исключение:** внешний ID, сам оканчивающийся на `-copy`, в
       linked-путь не допускается — DM такие ID разрешает
       (`services/disks/service.go:251`), но `GetNextDiskId(foo-copy)`
       вернёт `foo`, а не `foo-copy-copy`, и `GetLogicalDiskId` склеит его с
       чужим `foo`. DM отвергает такой запрос на входе приватного rpc.
       Инвариант alias'а: `TLeaderDiskInfo` в состоянии `Principal` на B
       **сохраняется** после финализации — по нему проверяется
       принадлежность `<id>-copy` (оговорка 2).
- **Fill generation.** `Clone` задаёт `FillGeneration > 0`; follower
  отвергает создание link, пока fill не завершён
  (`volume_actor_follower.cpp:128` → `IsVolumeOperationRestricted`,
  `volume_state.cpp:1287`). `FinishFillDisk` DM вызывает только в `Finishing`
  (`migrate_disk_task.go:491`) — цикл ожидания. Убрать fill generation
  нельзя: на нём держится условное удаление B при отмене
  (`deleteDstDisk` удаляет только при `FillGeneration > 0`,
  `migrate_disk_task.go:388`, через `DeleteWithFillGeneration` — чтобы
  запоздалый Cancel не удалил B новой попытки). **Решение:** копия создаётся
  с fill generation, как сейчас, а DM вызывает `FinishFillDisk` **сразу
  после `Clone`, до `CreateVolumeLink`**: защита от чужих записей на время
  копирования — тег `source-disk-id` плюс гейт follower'а (пускает только
  copy-клиентов), fill generation для этого не нужен. Но между
  `FinishFillDisk` и `CreateVolumeLink` у B нет владельца: `LinkUUID`
  появляется только на последнем шаге, `DeleteWithFillGeneration` после
  `FinishFillDisk` уже не сработает, а повторный `Clone` с тем же
  `<id>-copy` увидит finished-том и сочтёт его чужим
  (`multi_zone_client.go:90`). **Решение:** DM генерирует attempt token
  **до** `Clone`, пишет его в тег B при создании (`attempt=<token>`), и
  удаляет B только compare-and-delete по этому тегу (новое условие в
  `DestroyVolume` или action). Тот же token становится `LinkUUID`:
  `CreateVolumeLink` принимает его в запросе вместо генерации на A
  (`volume_actor_leader.cpp:173`) — тогда и «UUID теряется при потере
  ответа» (шаг 8) снимается само.
- **Порядок финализации.** Сейчас DM удаляет A сразу после статуса
  (`migrate_disk_task.go:534`); для linked-ветки — только после того, как
  старые endpoint'ы переключились (см. «Протокол переезда»). Момент самого
  переключения выбирает NBS, DM его только наблюдает по `getlinkstatus`.

Новый rpc в `PrivateService`, например
`MigrateDiskBetweenCells{disk_id, dst_cell_id}`, создаёт `migrateDiskTask`
с linked-протоколом; флаг `is_migration_between_cells` на публичном
`MigrateDisk` (`disk_service.proto:151`) остаётся только для старой ветки
под флагом конфига. Шаги задачи:
`Clone` с ID `<id>-copy` → `FinishFillDisk` → `CreateVolumeLink` в A
(добавить в Go-клиент NBS; в SDK сейчас нет) → опрос `getlinkstatus` в A →
финализация по протоколу ниже. Старая ветка с `ReplicateDisk` — под флаг
конфига, по умолчанию выключена; оба пути ставят `source-disk-id`, поэтому при
включённом флаге DM проверяет, что у диска нет активной миграции.

Ещё три вещи в DM, без которых ветка не доживёт до продакшена:
- **Выбранный протокол сохраняется в состоянии задачи**
  (`MigrateDiskTaskState`, `migrate_disk_task.proto:38`), а не читается из
  флага на каждом шаге: иначе смена флага посреди задачи переключит протокол
  после `Clone`.
- **Прогресс.** `MigrateDiskMetadata` берёт `Progress`/`SecondsRemaining`/
  `UpdatedAt` из `ReplicateTaskID` (`migrate_disk_task.go:177`); у
  linked-ветки его нет. Compute операцию не вызывает, так что его ожидания
  `UpdatedAt`/`SecondsRemaining` (`compute-api/.../instances/relocate.go:442`)
  не затрагиваются. Для оператора: прогресс — `MigratedBytes` из
  `getlinkstatus`, в метаданных приватной операции. Сценарий раскатки:
  задачи старого протокола, запущенные до переключения, дорабатывают по
  старому.
- **Сигналы** (`SendMigrationSignal`, публичный API). Сегодня
  `FINISH_REPLICATION` принимается для любой миграции в `Replicating`
  (`service.go:886`) и в `ReplicateDisk` между ячейками служит ручным
  ускорением завершения наряду с автоматическим порогом
  (`replicate_disk_task.go:62`). В linked-ветке торопить нечего — таблетка
  льёт до конца сама. **Решено:** отвечать ошибкой «сигнал не применим к
  миграции через linked volumes». `FINISH_MIGRATION` между ячейками сегодня
  не используется (переход автоматический); в linked-ветке момент
  переключения выбирает NBS, ручного подтверждения оператором нет, так что
  `FINISH_MIGRATION` остаётся неиспользуемым.

### Протокол переезда (cutover)

Это главное, чего не хватало: отмена, передача роли и переключение старых
endpoint'ов — один протокол с одним необратимым рубежом.

**Рубеж** — переход follower'а в `Leader` в его таблетке (B). До него переезд
отменяем, после — нет: A становится устаревшим, B — источник истины. Рубеж
проходится **самим NBS, как сегодня**: по окончании фонового копирования
(`IsMigrationFinished`) лидер персистит `DataReady`, что переводит A в
`LeadershipTransferring` (`volume_state.cpp:1221`) — новые записи отвергаются
`E_REJECTED` (`volume_actor_forward.cpp:784`) — и только после ответа на
этот персист шлёт COMPLETED в B (`follower_disk_actor.cpp:378-384`). DM не
выбирает момент переключения; ему это и не нужно, если смена внешнего ID
решается alias'ом или pending-метаданными, записанными **до** cutover с
откатом при отмене (см. модель ID). Поэтому никакого `WaitingForCutover` /
`TransferLeadership` — переключение идёт как внутри ячейки.

1. **До рубежа** (link создан, данные льются, `Preparing`). Отмена =
   `DestroyVolumeLink` в A, затем удаление B. NBS может перейти рубеж в
   любой момент после окончания копирования, поэтому DM перед отменой
   обязан спросить `getlinkstatus` в A: `LEADERSHIP_TRANSFERRING` и дальше —
   отмена невозможна (`Cancel` в `migrate_disk_task.go:149` сейчас решает по
   своему статусу — недостаточно).
2. **Барьер записей перед рубежом — нужен.** `IsMigrationFinished`
   считает только фоновые диапазоны
   (`part_nonrepl_migration_common_actor_migration.cpp:148`), зеркальные
   write/zero идут асинхронно (`..._mirror.cpp:115`,
   `WriteAndZeroRequestsInProgress`). Зеркальная запись подтверждается
   клиенту после ответа обеих сторон (`migration_request_actor.h:180-213`),
   и при retriable-ошибке B клиент получает её и повторит — это нормально.
   Отказ copy-клиенту на уже-`Leader` B — это `E_REJECTED` (follower в
   `Leader` получает статус `Principal`, `volume_state.cpp:1244-1248`,
   ветка `Principal` → `E_REJECTED`, `volume_actor_forward.cpp:764`),
   retriable (`error.cpp:51`) — здесь клиент получит ошибку и повторит.
   Но при **fatal** ошибке B от других причин (`E_PRECONDITION_FAILED`,
   `E_ARGUMENT`, gRPC `UNIMPLEMENTED`) `Done` возвращает клиенту
   `LeaderResponse` — успех A (`migration_request_actor.h:188-192`): клиент
   считает запись сделанной, а на B её нет. Внутри ячейки окно микросекундное, между ячейками — RTT.
   Поэтому перед COMPLETED нужен drain, и он должен **пережить рестарт**:
   `WriteAndZeroRequestsInProgress` живёт в памяти
   (`part_nonrepl_migration_common_actor.h:167`), а после рестарта
   `ApplyLinkState` переводит сохранённый `DataReady` в `DataTransferred` и
   шлёт COMPLETED сразу (`follower_disk_actor.cpp:215`) — незавершённые
   записи и их ошибки потеряны. Значит, drain нельзя делать *после*
   персиста `DataReady`. Новое персистентное состояние
   `TFollowerDiskInfo::EState::Draining` между `Preparing` и `DataReady`:
   персист `Draining` → новые записи `E_REJECTED`, отмена запрещена; актор
   ждёт опустошения списка; при fatal-ошибке зеркальной записи во время
   drain'а — `Error` **допустим** (мы ещё до рубежа, B не `Leader`), A
   возвращается в `Principal`, DM видит ошибку и повторяет/отменяет; после
   успешного drain'а — персист `DataReady` и COMPLETED как сегодня.
   **Восстановление после рестарта в `Draining` не может доверять пустому
   списку.** В `Done` ответ клиенту уходит *раньше* completion родителю, и
   они идут разным акторам (`migration_request_actor.h:206-213`): при
   fatal-ошибке B клиент получает успех A, а если таблетка упадёт между
   ответом клиенту и обработкой completion (`..._mirror.cpp:21`), ошибка
   потеряна — клиент считает запись сделанной, на B её нет, и список после
   рестарта пуст. Поэтому после рестарта в `Draining` A **ресинхронизирует**
   B перед `DataReady`. Ресинк — это **полный проход по диску**, дёшевым он
   не будет: `NonZeroRangesMap` не персистится, при создании помечает все
   диапазоны изменёнными (`changed_ranges_map.cpp:22`), и планировщик её
   не читает — `GetNextMigrationRange` идёт по `ProcessingBlocks` с нуля
   (`..._migration.cpp:166-215`); карта служит только для
   `GetChangedBlocks` наружу. Каждый диапазон читается с A и пишется в B
   или зануляется (`copy_range.cpp:86`). Держать записи закрытыми на это
   время нельзя: для 1 TiB при 100 MiB/s это порядка трёх часов, а
   повторный рестарт начинает проход заново. **Поэтому восстановление идёт
   в два шага:** из `Draining` A возвращается в `Preparing`-подобное
   состояние `Resyncing` (персистентное) — записи **открыты** и
   зеркалируются, фоновый проход идёт по всему диску; по его окончании —
   снова `Draining` (короткий: только записи в полёте), затем `DataReady`.
   Пауза записей при этом — один drain, как в штатном пути. Долговечный
   прогресс прохода не нужен: повторный рестарт в `Resyncing` просто
   начинает проход заново при открытых записях. Проверить отдельно:
   обнуление диапазона, который ранее был непустым на B, и двойной
   рестарт. Для нового
   межъячеечного link дополнительно: при fatal-ошибке B не отвечать
   клиенту успехом A, а отвечать ошибкой B — внутри ячейки это поведение
   менять не нужно. Тест: fatal B → успех A клиенту → crash до completion →
   recovery → запись есть на B. `getlinkstatus` отдаёт `Draining` как
   `LEADERSHIP_TRANSFERRING`.
   **Список drain'а не охватывает весь входной путь.** При `TrackUsedBlocks`
   или light checkpoint volume actor принимает write и передаёт его в
   migration actor через промежуточный `TWriteAndMarkUsedActor`
   (`volume_actor_forward_trackused.cpp:34`, `forward_write_and_mark_used.h:132`);
   `Draining` может застать `WriteAndZeroRequestsInProgress` пустым, а
   запись придёт позже. Барьер — на входе в migration actor: после персиста
   `Draining` любой write/zero, пришедший в `TFollowerDiskActor`,
   отвергается `E_REJECTED` независимо от того, как он добрался. Тест:
   задержать промежуточный актор, начать `Draining`, доставить запись. Фенсинг: `DataReady` на A до RPC в B
   (`follower_disk_actor.cpp:177-184, 378-384`); при восстановлении A
   сверяет состояние B по `LinkUUID`. Порядок фиксации роли (follower
   фиксирует и отвечает, A сохраняет `LeadershipTransferred` после —
   `follower_disk_actor.cpp:279, 390`, `volume_actor_follower.cpp:171`)
   остаётся как есть.
3. **Рубеж**: на follower'е одной транзакцией localdb — `Leader` плюс пометка
   «тег снять». Снятие `source-disk-id` идёт через SchemeShard
   (`service_actor_actions_modify_tags.cpp:239`) и не может быть в той же
   транзакции, поэтому follower после рестарта **доделывает** снятие тега по
   этой пометке (идемпотентно). Окно «тег ещё не снят» клиента не задевает:
   переключение идёт по `PrincipalDiskId` + `PrincipalCellId` из describe A
   (см. модель ID), а не через поиск B по всем ячейкам; cells-describe с
   фильтром по тегу нужен только для **новых** endpoint'ов, и для них B
   должен быть уже виден — DM удаляет A лишь после подтверждения снятия
   тега (шаг 6).
4. **Лидер** переходит в `LeadershipTransferred` и на remount отдаёт
   `PrincipalDiskId` (`switchable_client.cpp:284`). Два пути обратно в
   `Principal` после рубежа, оба надо закрыть:
   - параллельный `Unlink` на A (`volume_actor_leader.cpp:202`) → `E_REJECTED`
     начиная с **`Draining`**, не с `DataReady`: отмена запрещена с
     `Draining` (шаг 2), а `DestroyVolumeLink`, начатый DM по старому
     статусу `Preparing`, может прибыть уже в `Draining` — проверка
     состояния и отказ в одной транзакции localdb. `Resyncing` при этом
     отменяем, как `Preparing` (B ещё не `Leader`, A `Principal`). Тест с
     задержанным Destroy;
   - **поздняя fatal-ошибка**: `OnMigrationError` персистит `Error`
     (`follower_disk_actor.cpp:186`), а `Error` численно старше
     `DataReady`/`LeadershipTransferred` в enum (`follower_disk.h:94`), так
     что guard `newState >= State` (`follower_disk_actor.cpp:240`) её
     пропускает; `UpdateLeadershipStatus` (`volume_state.cpp:1221`) при
     `Error` не видит передачи роли и снова считает A `Principal` — хотя B
     уже `Leader`. Правка: после `DataReady` переход в `Error` запрещён;
     ошибка после рубежа — это «B недоступен», A остаётся
     `LeadershipTransferring` и повторяет COMPLETED. Отдельный тест на гонку.
5. **Старые endpoint'ы.** Переключение срабатывает только на успешном
   remount с `PrincipalDiskId`; если A удалить раньше, endpoint'у негде
   получить редирект. **Решено:** после рубежа лидер сам принуждает клиентов
   к remount — отвечает на IO `E_BS_INVALID_SESSION` с `EF_OUTDATED_VOLUME`
   (уже есть для `LeadershipTransferred`, `volume_actor_forward.cpp:730-816`),
   а для клиентов без IO — через таймер remount сессии: `TSession` делает
   периодический remount с периодом из ответа на mount (`session.cpp:773-788`),
   а `PrincipalDiskId` в ответ кладётся из состояния `LeadershipTransferred`
   (`volume_state.cpp:1227-1231`, `volume_actor.cpp:495`) — проверено, таймерный
   remount его получит. A живёт, пока на нём есть клиенты. Критерий «ноль
   клиентов» слабый: клиент пропадает из списка и по обрыву соединения
   (`volume_actor_addclient.cpp:608`, stale), хотя endpoint всё ещё держит
   маршрут в A; `listclients` — monitoring action, а `StatVolume` в Go SDK
   отбрасывает `Clients` (`safe_client.go:179`), хотя в API они есть
   (`TStatVolumeResponse.Clients`, `volume.proto:734`) — нужен метод SDK,
   возвращающий `Clients`, работа в DM-клиенте. Нужен сильный критерий:
   DM ждёт **ноль клиентов плюс `InactiveClientsTimeout` после рубежа** и
   только потом удаляет A. Период клиентского remount — это
   `ClientRemountPeriod` (4 с по умолчанию, `config.cpp:285`): именно его
   сервер кладёт в поле `InactiveClientsTimeout` ответа на mount
   (`volume_session_actor_mount.cpp:121`), а сессия им перемонтируется
   (`session.cpp:775`). Серверный `InactiveClientsTimeout` (9 с) — отдельное
   значение, после которого клиент без remount считается stale и выкинут с A
   независимо от переезда. Ждать нужно большее из двух, т.е. 9 с: после него
   любой живой endpoint либо переключился, либо уже не клиент A.
   Этого **недостаточно** само по себе: `SwitchSessionForEndpoint` при
   ошибке describe B просто прекращает попытку (`session_manager.cpp:1117`),
   endpoint остаётся на A и при следующем remount попробует снова — поэтому
   перед удалением A DM обязан подтвердить, что B **виден** через
   cells-describe (тег снят, см. шаг 3 и 6). Для endpoint'а, который так и
   не переключился, после удаления A спасает резолвинг `id → <id>-copy` в
   cells-describe (см. модель ID), а не редирект с A. Пока A жив и тег с B
   снят, cells-describe по `<id>` найдёт только A (B называется `<id>-copy`),
   а `<id>-copy` — только B. Коллизии нет **именно потому**, что ID разные;
   при одинаковом ID (`Clone` как сейчас) describe мог бы снова выбрать A
   (`describe_volume.cpp:328`).
6. **Финализация DM**: удалить A, переключить alias на `<id>-copy`
   (`DiskRelocated`), и только **после подтверждения, что тег
   `source-disk-id` с B снят**, — очистка link на B (`Principal`); иначе при
   retry теряется маркер, а B остаётся скрытым для cells-describe.
   Подтверждение — обычный `DescribeVolume` B по его ячейке: DM ходит к
   ячейке напрямую, фильтр по тегу есть только в cells-describe NBS, а теги
   отдаются в `TVolume.Tags` (`volume.proto:292`). Ничего нового не нужно.
7. **Восстановление после сбоя** на любом шаге: источник истины —
   `getlinkstatus` в A (до удаления A) и состояние link на B (после). DM
   хранит только «какой шаг последний подтверждён».
8. **Идентификатор попытки и ограждение.** Публичные
   `Create`/`DestroyVolumeLink` несут только пару DiskId (`volume.proto:822`),
   `Match` без UUID сравнивает по паре и shard'ам (`follower_disk.cpp:56`),
   поздний `UpdateFollowerState` с неизвестным UUID **заново добавляет**
   follower на A (`volume_actor_leader.cpp:273`), а UUID, сгенерированный
   на A, теряется, если ответ на `CreateVolumeLink` не дошёл до DM
   (`service_actor_create_volume_link.cpp:74`). Идемпотентность по паре ID
   **уже есть**: повторный `CreateVolumeLink` находит существующий link и
   отвечает «already exists» с его UUID (`volume_actor_leader.cpp:117-135`,
   `link = follower->Link`). Нужно только довести UUID до ответа публичного
   `CreateVolumeLink` (сейчас ответ пуст) — а лучше принимать UUID от DM в
   запросе (attempt token, см. раздел DM), тогда повтор идемпотентен по
   UUID, а не по паре ID;
   A персистит «активная/отменённая попытка» по UUID и отбрасывает события
   от отменённой; `DestroyVolumeLink` требует UUID; удаление B при отмене —
   условное по attempt token в теге (см. раздел DM). DM хранит UUID в
   состоянии задачи и передаёт во все вызовы.
   **Поздний CREATE отменённой попытки может захватить новый B:**
   CREATE(uuid1) задерживается, попытка 1 отменена и B удалён, попытка 2
   создаёт B с тем же физическим именем и `attempt=uuid2`; старый CREATE
   приходит раньше нового и сохраняет `Following(uuid1)`, после чего
   настоящий CREATE(uuid2) отвергается как «link уже есть»
   (`volume_actor_follower.cpp:113-125`). Проверка «UUID относится к
   известному link» здесь бессильна — CREATE эту запись и создаёт.
   Поэтому B до сохранения `Following` сверяет UUID из CREATE с attempt
   token в своём теге (он есть с момента `Clone`, см. раздел DM); не
   совпал — `E_REJECTED`. Тест: cancel → recreate → late CREATE.
   **`DestroyVolumeLink` сегодня не подтверждает состояние B:** A отвечает
   после своей транзакции, а follower'у шлёт уведомление отдельно, не дожидаясь
   (`volume_actor_leader.cpp:220-245`, «notify the follower just in case»).
   Для отмены нужен двусторонний идемпотентный результат: A дожидается ответа
   B и возвращает «B не `Leader`, link снят» либо ошибку; тест на сбой между
   локальным снятием и ответом B. Плюс per-disk owner в DM: пока идёт
   переезд, `Resize`/`Delete`/вторая миграция того же диска отвергаются.

### NBS

Порядок такой, что каждый этап сливается отдельно, а поведение внутри ячейки не
меняется до последнего.

**Этап 0. ShardId = CellId.** Заполнять `LeaderShardId` (из `AppCtx.CellId`,
`bootstrap.cpp:783-784`) и `FollowerShardId` (из запроса `CreateVolumeLink`;
поле надо добавить в публичный `TCreateVolumeLinkRequest`, `volume.proto:822`).
Внутри ячейки они равны, `GetDirectCopyUsage` уже сравнивает их — direct copy
через DiskAgent автоматически выключится для межъячеечного link. Общая часть.
Раскатка: новый сервис шлёт заполненные id, а таблетка старой версии
сравнивает их строго с пустыми — `getlinkstatus` даст `NOT_FOUND`, destroy —
`S_ALREADY` при живом link'е. Флаг не делаем: копирования запускаются
только руками, на время раскатки их не запускать (решение 2026-10-09).

**Этап 1. Мост storage → cells.** Сервис-актор `MakeCellProxyServiceId()` в
`libs/storage/api`: по `CellId` отдаёт пару `IBlockStorePtr` (control,
`ICellConnection::GetService()`) и `IStoragePtr` (data,
`ICellConnection::GetStorage()` → `TRemoteStorage`). Сессию мост не держит:
данные идут от `copy-volume-client` без маунта (этап 3). Реализация в `libs/cells` поверх `ICellManager::CreateConnection`, регистрация
в `libs/daemon/ydb/bootstrap.cpp` рядом с `CreateCellsMonActor`; без ячеек —
заглушка. По правилу «общая часть отдельно»: PR 1a — интерфейс и заглушка,
PR 1b — реализация.
Соединение — на каждый link, не разделяемое: мост делает
`CreateConnection(cellB, {} /* любой живой хост */, …)` и держит
`ICellConnectionPtr` до конца копирования или ошибки; при bootstrap data-
соединений нет, только `ICellManager` с пулами хостов и control-каналами
(`session_manager.cpp:905`, `cell_manager_impl.cpp:141`). Переехала
таблетка по hive — новый актор, новое соединение.
**Хост в B выбирается из пула, а не хост таблетки follower'а.** `TabletHost`
соединение узнаёт только из `MountVolumeResponse` (`connection.cpp:424`,
`OnMountResponse` → `MigrateTo`), а копирование идёт без маунта. Записи
в B пойдут через лишний хоп: volume proxy выбранного хоста → таблетка.
Внутри ячейки структура та же (volume proxy → pipe), так что это не
регресс. Опция, не обязательная: describe через мост отдаёт `TabletHost`
(поле в `DescribeVolumeResponse`), и мост после describe зовёт `MigrateTo`.
Решать на этапе 2/3 по замерам.

**Этап 2. Control через мост.** При `FollowerShardId != LeaderShardId`:
- Все describe follower'а идут через мост (`DescribeVolume` с `CellId`), а
  не через SSProxy. Их два, не один: `TCreateVolumeLinkActor`
  (`create_volume_link_actor.cpp:42-55`) и `TVolumeAsPartitionActor`, который
  сам описывает назначение перед копированием
  (`volume_as_partition_actor.cpp:37-50`).
- `propagate_to_follower.cpp`: `TEvUpdateLinkOnFollowerRequest` в чужую ячейку
  идёт через мост как **новый запрос `UpdateVolumeLink`** (public api +
  `TCellForwardService` whitelist, `forward_service.cpp:18-23`). На
  принимающей стороне он попадает в существующий обработчик
  `volume_actor_follower.cpp:309-374`. `LinkUUID` в запросе — проверка, что
  обновление относится к известному link; авторизация не рассматривается.
- Destroy старого лидера (`volume_actor_follower.cpp:239-249`) в чужую ячейку
  **не делаем**: источник удаляет DM. Principal в B лишь помечает link как
  `Principal`.
- `TControlService` штампует `CellId` только на Mount/Unmount
  (`connection.cpp:161`); для `UpdateVolumeLink` и `DescribeVolume` через
  мост штамп нужен тоже — иначе forward service на той стороне их не
  пропустит.
- `getlinkstatus` остаётся локальным в A — DM спрашивает ячейку лидера.
- **`DescribeVolumeResponse` от лидера** (общая часть, `libs/storage`):
  заполнять `Volume.PrincipalDiskId` на describe, добавить
  `PrincipalCellId`. Нюанс: сервисный describe читает SchemeShard, а
  `PrincipalDiskId` живёт в localdb таблетки (`service_actor_describe.cpp:140`,
  «Need localdb to get principalDiskId»), поэтому describe должен спрашивать
  таблетку (как `StatVolume`), либо лидер при переходе в
  `LeadershipTransferred` публикует principal в конфиг тома через
  SchemeShard. Второе проще для читателей, но это ещё одна запись в
  SchemeShard на cutover. Те же поля — в `MountVolumeResponse`.
- **cells-describe с фоллбэком на `<id>-copy`** (см. модель ID): только
  если `id` не найден ни в одной ячейке и не задан `ExactDiskIdMatch`. Без
  этого перезапущенный endpoint после удаления A не найдёт диск.
- Пронести `PrincipalCellId` **по всему пути переключения**, не только в
  describe: `ISessionSwitcher::SwitchSession` сегодня принимает два DiskId
  (`switchable_session.h:67`), `SwitchSessionForEndpoint` делает полный
  cells-describe B (`session_manager.cpp:1143-1158`), а `CreateSessionImpl`
  при рекурсивном переходе A → B (`session_manager.cpp:624-632`) теряет
  ячейку. Все три — принимать `PrincipalCellId` и описывать B в ней
  напрямую с `ExactDiskIdMatch`. Тест: переключение **до** снятия тега с B.


**Этап 3. Данные через мост без маунта.**
- Сегодня `copy-volume-client` работает только in-process, потому что по сети
  `WriteBlocks` без маунта отбивает **storage service**, а не таблетка:
  `service_actor_forward.cpp:121-149` требует маунченного клиента и сессию.
  Таблетка же `copy-volume-client` без маунта принимает
  (`volume_actor_forward.cpp:825-840`: только writes, только пока других
  клиентов нет). Проверено по всему пути: RDMA-target (`rdma_target.cpp:337`)
  разворачивает `WriteBlocks` в `WriteBlocksLocal` и шлёт в тот же стек
  сервисов, что и gRPC; ни он, ни `Auth` (data channel без авторизации,
  `auth_provider_kikimr.cpp:207`), ни `Encryption` (клиент без сессии идёт
  вниз как есть, `encryption_service.cpp:55-66`) своей проверки не делают.
  Единственный гейт — `service_actor_forward.cpp:121-149`.
- Правка: в `service_actor_forward.cpp` пропускать `WriteBlocks`/`ZeroBlocks`
  **и `WriteBlocksLocal`** с `ClientId = copy-volume-client` к тому без
  проверки сессии. `WriteBlocksLocal` — отдельный форвард
  (`service_actor_forward.cpp:240`), и именно через него идёт RDMA
  (`rdma_target.cpp:364`); исключение только для `WriteBlocks` оставило бы
  RDMA-путь закрытым, а gRPC fallback скрыл бы это. Нюанс: у
  `WriteBlocksLocal` данные лежат в C++ `Sglist` вне protobuf
  (`request.h:59`), а конвертация Local → `WriteBlocks` сегодня делается
  `CreateWriteBlocksRemoteActor` только для **маунченного** клиента
  (`service_actor_forward.cpp:48-60`, через `VolumeClientActor`). Если
  пропустить Local в volume proxy как есть, pipe сериализует только `Record`
  (`volume_proxy.cpp:485`) и payload потеряется. Поэтому bypass для
  copy-клиента обязан конвертировать Local в обычный `WriteBlocks` (копия
  `Sglist` в protobuf) **до** volume proxy, с сохранением `LinkUUID`. Признак
  «межъячеечный форвард» на data-пути сегодня **нечем** выставить:
  `TRemoteStorage` не ставит `CellId` (`remote_storage.cpp:30`, он только
  чистит `Internal`), а RDMA-target не штампует `RequestSource`
  (`rdma_target.cpp:140`). Поэтому: `TRemoteStorage` ставит `CellId` в
  заголовки data-запросов (как `TControlService` на Mount/Unmount), и в
  запрос добавляется `LinkUUID` — **в `THeaders`**, чтобы поле пережило и
  gRPC, и RDMA-преобразование в `WriteBlocksLocal` —
  storage service пропускает без сессии только `copy-volume-client` с
  `CellId`, а таблетка принимает запись только если `LinkUUID` совпадает с её
  link. Это привязывает запись к конкретному переезду. Проверка
  `LinkUUID` — **только для записей с `CellId`** (межъячеечных):
  внутриячеечный `TVolumeAsPartitionActor` ставит один `ClientId`
  (`volume_as_partition_actor.cpp:298`), и общая проверка отвергла бы
  локальное копирование. Подтверждение
  отправителя на уровне канала есть только у gRPC secure-порта (источник по
  порту); RDMA-target источник не штампует и стоит в стеке до
  `TCellForwardService` (`bootstrap.cpp:271`). **Решено:** копировать по RDMA
  без флага и без проверки источника; риск записан ниже.
- `TVolumeAsPartitionActor`: при чужой ячейке Write/Zero → `IStoragePtr`
  моста (`TRemoteStorage`) вместо volume proxy, с `ClientId =
  copy-volume-client`, как сейчас. RMW неполных блоков там
  `E_NOT_IMPLEMENTED` — как и внутри ячейки.
- Транспорт — data-канал соединения: RDMA с gRPC fallback по конфигу ячейки,
  тот же, что у endpoint'ов. Новых запросов нет.
- Маунт на follower'е не нужен: ни сессии в мосту, ни unmount при передаче
  лидерства.

**Этап 4. Cutover.** Перед реализацией — отдельный дизайн-документ с
**явной таблицей переходов** `TFollowerDiskInfo::EState` для
межъячеечного link, включая `Preparing → Draining`, `Draining → Resyncing`
(только по рестарту), `Resyncing → Draining`, `Draining → DataReady`, и
кто инициирует каждый; текущий числовой guard `newState >= State`
(`follower_disk_actor.cpp:240`) для циклов `Draining ↔ Resyncing` не годится
и заменяется явной таблицей. Прогресс-колбэки сохраняют `Resyncing`;
`getlinkstatus` отдаёт `Resyncing` как `PREPARING`, `Draining` как
`LEADERSHIP_TRANSFERRING`; recovery-проход всегда с индекса 0.
Обязательные проверки: двойной рестарт в `Resyncing`; write/zero
параллельно полному проходу; обнуление ранее непустого на B диапазона;
поздний write на входе в `Draining`; задержанный `Destroy` на переходе в
`Draining`; невозврат A в `Principal` после `DataReady`; fatal B → успех A
клиенту → crash до completion → recovery.
Реализация «Протокола переезда»: на лидере —
состояния `Draining`/`Resyncing` с барьером на входе в migration actor,
отказ `Unlink` с `Draining` и запрет `Error` после `DataReady`; на follower'е — `Leader` + пометка «снять тег»
одной транзакцией и доделывание снятия после рестарта; `DestroyVolumeLink`
с подтверждением от B; DM — проверка `getlinkstatus` перед отменой,
подтверждение видимости B, ожидание нуля клиентов плюс 9 с на A перед
удалением.

**Этап 5. Наблюдаемость.** Страница `/blockstore/cells`: текущие переезды
(лидер, follower, ячейки, прогресс из `TFollowerDiskInfo`). Сенсоры
`MovesInProgress`, `MoveFailures` в `TCellCounters`.

**Этап 6. Тесты и раскатка.**
- `volume_ut_linked.cpp` с фейковым мостом: describe follower в чужой ячейке,
  пропагация, записи через `IStoragePtr`, передача лидерства, снятие тега.
- `libs/cells`: мост на двух реальных серверах с разными `CellId` (по образцу
  `ShouldServeGrpcDataThroughControlPort`): `UpdateVolumeLink` через forward
  service, `WriteBlocks` от `copy-volume-client` без маунта по data-каналу
  (gRPC и RDMA), отказ при маунченном другом клиенте.
- DM: facade-тест по образцу `disk_service_cells_test.go` с новой веткой.
- Раскатка: `FollowerShardId` (этап 0), `UpdateVolumeLink` и пропуск
  `copy-volume-client` без сессии (этапы 2–3) должны быть во всех ячейках раньше, чем DM
  включит новую ветку. Та же логика, что с
  `TabletHost`: поле — признак готовности.

## Риски и открытые вопросы

1. **Таблетка лидера переехала по hive** на другой хост — соединение моста
   живёт на хосте. Мост по `CellId` на каждом хосте это решает; первый запрос
   после переезда платит за установку соединения.
2. **Alias в DM.** Внешний `id` → физический `<id>-copy` после переезда.
   Работа целиком внутри DM, но затрагивает все вызовы NBS по диску; пропуск
   одного места — обращение к несуществующему диску. Нужен владелец DM и
   полный список таких вызовов. Повторный переезд чередует имя
   `id ↔ <id>-copy` (`GetNextDiskId`), третьего имени не бывает.
3. **Cutover** — см. «Протокол переезда»; главный риск — состояние,
   разложенное по четырём местам (localdb A, localdb B, SchemeShard-теги,
   статус DM), и сбой между шагами. Источник истины зафиксирован: NBS, не DM.
4. **`copy-volume-client` без сессии по сети** — ослабление проверки в
   storage service. Радиус: записи в диск, который никем не маунчен, помечен
   `source-disk-id` и имеет link с совпадающим `LinkUUID`.
5. **Encryption, root-KMS — решено.** KMS один на все ячейки, KEK общий,
   поэтому перенос — это создать B с **тем же `EncryptionDesc`**, что у A, а
   не с новым DEK. Сегодня `Clone` передаёт только `Mode` + `KeyHash`
   (`multi_zone_client.go:156`), а `CreateVolume` для root-KMS всегда
   генерирует новый DEK (`service_actor_create.cpp:208-212, 392-398`) и
   кладёт в конфиг тома `KekId` + зашифрованный DEK
   (`TVolumeConfig.EncryptionDesc`, `blockstore_config.proto:28`); клиент при
   маунте расшифровывает его через root-KMS по `KekId`
   (`encryption_client.cpp:808-850`). Нужно: `CreateVolume` принимает готовый
   `EncryptedDataKey` в `EncryptionSpec` и не генерирует новый; DM при
   `Clone` для linked-ветки копирует `EncryptionDesc` A целиком; после
   `Clone` DM сверяет `EncryptionDesc` обеих сторон и останавливается при
   несовпадении — это же закрывает auto-KMS на B
   (`service_actor_create.cpp:165-185`). DEK не привязан к DiskId: `diskId`
   в `GenerateDataEncryptionKey`/`GetKey` игнорируется
   (`encryption_key.cpp:231`, `root_kms/iface/client.cpp:26,36`), так что
   DEK диска `id` расшифруется и под `<id>-copy`. Продовый клиент есть в
   `root_kms/impl/client.cpp` и шлёт в KMS только `key_id` + `ciphertext`
   (`:79, :136`), DiskId не передаёт — подтверждать у KMS нечего.
   **Формат:** `Ciphertext` в конфиге
   тома уже Base64 (`service_actor_create.cpp:397`), публичный describe отдаёт
   его как есть (`proto_helpers.cpp:121`), клиент декодирует
   (`encryption_client.cpp:830`). `Clone` должен передать строку без
   повторного кодирования, а `CreateVolume` при готовом ключе — положить
   её в конфиг без `Base64Encode`.
6. **Параллельный запуск двух путей** на одном диске (если флаг включат): оба
   ставят `source-disk-id`. Достаточно проверки в DM, что у диска нет активной
   миграции.
7. **Авторизация** межъячеечных запросов сознательно отложена; `LinkUUID` в
   `UpdateVolumeLink` и в copy-записях — проверка целостности и fencing
   попыток, не защита от злонамеренного peer'а.
8. **Copy-записи по RDMA без проверки источника.** Любой, кто дотянется до
   RDMA-порта хоста и знает `DiskId` + `LinkUUID` follower'а, может писать в
   него до передачи лидерства. `LinkUUID` **не секрет**: он виден на
   mon-странице тома (`volume_actor_monitoring.cpp:270`) и по плану
   возвращается в ответе `CreateVolumeLink` / передаётся из DM. Так что
   радиус — не угадывание, а «любой, кто имеет сетевой доступ к RDMA-порту
   и читает мониторинг». Серверно проверяемого «это ячейка A» на RDMA нет.
   Принято сознательно по решению владельца; закрыть, когда у RDMA-канала
   появится доверенный источник (тот же вопрос, что и для control на RDMA).
9. **Принудительный remount после рубежа** опирается на `EF_OUTDATED_VOLUME`
   в ответах на IO и на таймер remount в `TSession`. Клиент без IO
   переключится не раньше периода таймера; DM ждёт как минимум его (протокол,
   шаг 5). Endpoint, отставший дольше, получит `E_NOT_FOUND` — принято.

## Оценка

NBS: этапы 0–1 небольшие; этап 2 — основной объём; этап 3 — малый (исключение в
storage service плюс замена назначения в `TVolumeAsPartitionActor`); этапы 4–5 — малые. DM: одна новая ветка в
задаче миграции, клиентский вызов `CreateVolumeLink`, опрос статуса, флаг.
Тесты сопоставимы с кодом.
