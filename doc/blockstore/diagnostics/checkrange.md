# Утилита checkarange

## Описание
Является частью blockstore-client приложения и запускается стандартным для него образом: `blockstore-client.sh checkrange`.
Утилита подсчитывает чексуммы каждого запрощенного блока и проверяет целостность данных указанных блоков для каждого поддерживаемого типа диска:
- non replicated disk (чтение данных)
  - посчитать чексуммы
  - проверить возможность чтения
- mirror2 / mirror3 (чтение данных + сверка чексумм реплик)
  - проверка чтения данных с каждой реплики
  - сравнение и вывод чексумм данных между репликами
- replication
  - проверка чтения данных (в ydb будет своя неявная проверка целостности)
  - TODO для проверки ssd диска запланирована отдельная команда в ydb - https://github.com/ydb-platform/ydb/issues/8189


## Возможности
Утилита поддерживает следующие опции:
### в любом слуае выполняется
- запрос на подсчёт чексуммы каждого указанного блока. Например, на диапазон из 1024 блоков вернется 1024 чексуммы
### обязательные параметры
- "disk-id" - указание идентификатора диска
### Стандартные опции запуска
- "start-index" - стартовый индекс блока. По умолчанию: 0.
- "blocks-count" - количество блоков. По умолчанию: количество блоков до конца диска, начиная со start-index
- "blocks-per-request" - количество блоков в 1 запросе. По умолчанию: 1024
- "output" - файл для сохранения результатов. По умолчанию: stdout

## Базовые сценарии использования
- миграция данных и сравение чексумм
  - остановили запись на диск
  - посчтитали чексуммы
  - смигрировали данные на другой диск (мб даже другого типа)
  - сравнили чексуммы

TODO добавить примерный парсер на питоне или чем-то подобном

## Формат ответа
{
  "ranges": [
    {
      "r_start":0,
      "r_end":1023,
      "error": {}, // если была ошибка
      "checksums": [], // если их удалось получить
      "mirror_checksums": [ //для mirror дисков в случае расхождения чексумм хотя бы для 2 реплик
        {
          "ReplicaName":"name",
          "Checksums": [1,2,3,4,5]
        }
      ]
    }
  ],
  "summary": {
    "requests_num":1,
    "errors_num":0,
    "problem_ranges": [ // если ошибки были в 2 последовательных диапазонах, они мёржатся в 1 большой
      {
        "r_start":0,
        "r_end":2046
      }
    ],
    "global_error": {
      "Code": int,
      "CodeString": string,
      "Message": srting,
      "Flags": int
    },
  }
}

## Внутреннее устройство / логика работы
### На стороне утилиты
* получение информации о диске
  * на основе размера диска и изначально запрошенного количества блоков выставляется нужное количество блоков
* для каждого диапазона блоков последовательно выполняется запрос checkRange, который далее будет обрабатываться соответсвующим актором на сервере (например, начальной точкой обработки может быть part_mirror_actor_checkrange.cpp: HandleCheckRange)
* обработка ответа
  * проверка "верхнеуровневых" ошибок обработки аргументов
  * проверка ошибок чтения
  * выставление чексумм для каждого требуемого диапазона

### На стороне сервера

### nonreplicated disk
* TNonreplicatedPartitionActor::HandleCheckRange
  * проверяет, что запрошенное количество блоков меньше максимально допустимого размера
  * регистрирует актор TNonreplCheckRangeActor, работающий через методы родительского актора (в partition_common), используемого для partition

#### mirror диски
* TMirrorPartitionActor::HandleCheckRange
  * проверяет, что запрошенное количество блоков меньше максимально допустимого размера
  * регистрирует актор TMirrorCheckRangeActor, передаёт ему имена (DeviceUUID) реплик
  * TMirrorCheckRangeActor
    * SendReadBlocksRequest: отправляет запросы на чтение указанных блоков каждой реплики mirror'а (TEvReadBlocksRequest).
      * TMirrorPartitionActor::HandleReadBlocks(part_mirror_actor_readblocks.cpp): обработка запроса на чтение -> TMirrorPartitionActor::ReadBlocks
        * из хедеров выбираем нужную реплику
        * создается `TRequestActor<TReadBlocksMethod>` для чтения из нужной реплики
          * SendRequests из созданного на предыдущем шаге актора
            * обработка ответов происходит в HandleResponse, в итоге выполняем ставим чексумму и возвращаем ответ
    * В обработке ответа (TMirrorCheckRangeActor::HandleReadBlocksResponse)
      * проверка ошибок чтения с реплики
      * вычисление чексумм диапазона
      * подготовка ответа с 1 общей чексуммой или набором чексумм, если были расхождения

#### partition диски
* TPartitionActor::HandleCheckRange
  * проверяет, что запрошенное количество блоков меньше максимально допустимого размера
  * регистрирует актор TCheckRangeActor (базовый класс для других CheckRangeActor'ов)

  * TCheckRangeActor: Bootstrap + SendReadBlocksRequest
    * отправляем запрос на чтение указанных блоков в Partition (TEvReadBlocksLocalRequest).
      * TPartitionActor::HandleReadBlocksLocal -> HandleReadBlocksRequest
        * создаётся запрос на чтение блоков
        * для последующего возврата ответа регистрируем TReadBlocksLocalHandler через CreateReadHandler
        * в TPartitionActor::ReadBlocks выполняется чтение блоков через транзакцию CreateTx<TReadBlocks> с хендлером TReadBlocksLocalHandler
        * в конце транзакции вызывается TPartitionActor::CompleteReadBlocks
          * регистрируется актор TReadBlocksActor
            * // в процессе выполнения будет происходить работа с TReadBlocksLocalHandler::GetGuardedSgList с вызовом из TReadBlocksActor::ReadBlocks
            * отправляется TEvReadBlobRequest
            * TReadBlocksActor::HandleReadBlobResponse
              * проверяем чексуммы в VerifyChecksums для каждого ответа
              * дожидаемся всех ответов
              * генерируется ответ через CreateReadBlocksResponse
    * В обработке ответа (TCheckRangeActor::HandleReadBlocksResponse) проверит ошибки и посчитает + проставит полученные чексуммы

## Юниттесты
### volume
Содержит в себе общую для всех типов партиционирования логику. Тип диска получает параметром.
#### Позитивные тесты
- DoTestShouldCommonCheckRange: получение одного общего ответа на checkRange + проверка чексумм
- DoTestShouldSuccessfullyCheckRangeIfDiskIsEmpty: проверка checkRange для пустого диска
- DoTestShouldGetSameChecksumsWhileCheckRangeEqualBlocks: сверяем чексуммы одинаковых данных разных блоков
- DoTEstShouldGetDifferentChecksumsWhileCheckRange: проверяем отличие чексумм разных блоков
- DoTestShouldCheckRangeIndependentChecksum: проверка независимости чексумм соседних блоков

#### Негативные тесты
- DoTestShouldCheckRangeWithBrokenBlocks: проверка ошибки "block is broken" через подмену перехваченного внутреннего ответа
- DoTestShouldntCheckRangeWithBigBlockCount: проверка обработки параметра для слишком большого размера блока

### nonreplicated
- Все тесты реализованы как "общие" в volume

### mirror disk
#### Позитивные тесты
- ShouldMirrorCheckRangeRepliesFromAllReplicas: получение промежуточных ответов с каждой из 3 реплик. Необходим для проверки участия каждой реплики
- Остальные тесты реализованы как "общие" в volume
#### Негативные тесты
- ShouldMirrorCheckRangeOneReplicaBroken: проверка ошибки "block is broken" на 1 реплике через подмену перехваченного внутреннего ответа

### partition disk
- Все тесты реализованы как "общие" в volume
