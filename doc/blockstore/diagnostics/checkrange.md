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

## Формат ответа
{
  "ranges": [
    {
      "success":true/false,
      "r_start":0,
      "r_end":1023,
      "error": {},
      "checksums": [],
      "mirror_checksums": [ //для mirror дисков в случае расхождения чексумм хотя бы для 2 реплик
        {
          "ReplicaName":"",
          "Checksums": []
        }
      ]
    }
  ],
  "summary": {
    "requests_num":1,
    "range_errors":0,
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
  * для mirror2/mirror3 дисков выставляется количество реплик
  * на основе размера диска и изначально запрошенного количества блоков выставляется нужное количество блоков
* для каждого диапазона блоков последовательно выполняется запрос checkRange, который далее будет обрабатываться соответсвующим актором (например, начальной точкой обработки может быть part_mirror_actor_checkrange.cpp: HandleCheckRange)
* обработка ответа
  * проверка "верхнеуровневых" ошибок обработки аргументов
  * проверка ошибки чтения. В случае mirror диска будет повторная попытка чтения.
    *TODO учитывать количество преплик, а не просто и наличие* Мб вообще убрать эти перезапросы?
  * при наличии соответствующих параметров происходит
    * сверка полученных чексумм с уже имеющимися. Для этого необходимо запросить чексуммы в текущем запросе и иметь ранее сохраненные на диск чексуммы
    * сохранение ответа на диск

### На стороне сервера

### nonreplicated disk
* TNonreplicatedPartitionActor::HandleCheckRange
  * проверяет, что запрошенное количество блоков меньше максимально допустимого размера
  * регистрирует актор TNonreplCheckRangeActor

  * TNonreplCheckRangeActor: TNonreplCheckRangeActor::Bootstrap + SendReadBlocksRequest
    * отправляем запрос на чтение указанных блоков в nonrepl Partition (TEvReadBlocksLocalRequest).
      * TNonreplicatedPartitionActor::HandleReadBlocksLocal
        * создаётся запрос на чтение блоков
        * регистрируется актор TDiskAgentReadLocalActor
          * TDiskAgentReadLocalActor
            * TDiskAgentReadLocalActor::SendRequest
              * отправляет TEvReadDeviceBlocksRequest запросы на чтение блоков
            * TDiskAgentReadLocalActor::HandleReadDeviceBlocksResponse
              * копирует блоки данных
              * проставляет чексуммы
        * не регистрирует обработчик ответа, он возвращается сквозным образом

    * В обработке ответа (TNonreplCheckRangeActor::HandleReadBlocksResponse) проверит ошибки и, если был задан соответсвующий параметр, посчитает и проставит полученные чексуммы для каждого блока
      * не переиспользуется checkSum от обрабатываемого тут внутреннего запроса, тк там 1 чексумма на весь диапазон, а в checkRange нужны чексуммы на каждый блок. При этом с тз использования ресурсов лучше запросить диапазон данных и посчитать чексуммы для каждого блока, нежели делать N запросов чексумм к диску.

#### mirror диски
* TMirrorPartitionActor::HandleCheckRange
  * проверяет, что запрошенное количество блоков меньше максимально допустимого размера
  * регистрирует актор TMirrorCheckRangeActor

  * TMirrorCheckRangeActor
    * *TODO почти копипаста базового метода. Сделать рефакторинг*
    * отправляем запрос на чтение указанных блоков в Partition (TEvReadBlocksLocalRequest).
      * TMirrorPartitionActor::HandleReadBlocksLocal (part_mirror_actor_readblocks.cpp): обработка запроса на чтение -> TMirrorPartitionActor::ReadBlocks
        * из хедеров получаем количество реплик
        * выбираются акторы реплик для последющих запросов
        * создается `TRequestActor<TReadBlocksLocalMethod>` для координации чтения из нескольких реплик
          * SendRequests из созданного на предыдущем шаге актора
            * идём в реплики с запросами TEvChecksumBlocksRequest (кроме 0 реплики) и чтением в нулевую
            * обработка ответов происходит в 2 методах HandleChecksumResponse + HandleResponse, в итоге выполняем сравнение чексумм и возвращаем ответ
          * для mirror3 указываем "уровень" расхождения данных - Major/Minor (отличие для 1 или 2 реплик)
    * В обработке ответа (TMirrorCheckRangeActor::HandleReadBlocksResponse) проверит ошибки и, если был задан соответсвующий параметр, посчитает и проставит полученные чексуммы

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
    * В обработке ответа (TCheckRangeActor::HandleReadBlocksResponse) проверит ошибки и, если был задан соответсвующий параметр, посчитает и проставит полученные чексуммы

## Юниттесты

### nonreplicated
#### Позитивные тесты
- ShouldCheckRangeWithCheksums: получение одного общего ответа на checkRange + проверка чексумм
- ShouldCheckRangeInternal: проверка перехваченных внутренних ответов
- ShouldSuccessfullyCheckRangeIfDiskIsEmpty: проверка checkRange для пустого диска
- ShouldGetSameChecksumsWhileCheckRangeEqualBlocks: сверяем чексуммы одинаковых данных разных блоков
- ShouldGetDifferentChecksumsWhileCheckRange: проверяем отличие чексумм разных блоков
- TODO попробовать написать тест DifferentDisks в volume или переиспользовать mirror

#### Негативные тесты
- ShouldCheckRangeWithBrokenBlocks: проверка ошибки "block is broken" через подмену перехваченного внутреннего ответа
- ShouldntCheckRangeWithBigBlockCount: проверка обработки параметра для слишком большого размера блока

### mirror disk
#### Позитивные тесты
- ShouldMirrorCheckRange: получение одного общего ответа на checkRange
- ShouldMirrorCheckRangeWithCheksums: получение хэшсумм (тест работы переданного в запросе параметра)
- ShouldMirrorCheckRangeRepliesFromAllReplicas: получение промежуточных ответов с каждой из 3 реплик. Необходим для проверки участия каждой реплики

#### Негативные тесты
- ShouldMirrorCheckRangeBroken: тесткейс на отличающиеся хэшсуммы

### partition disk
#### Позитивные тесты
- ShouldCheckRange: пишем в различные диапазоны диска, проверяем checkRange
- ShouldSuccessfullyCheckRangeIfDiskIsEmpty: проверка checkRange для пустого диска
- ShouldGetSameChecksumsWhileCheckRangeEqualBlocks: сверяем чексуммы одинаковых данных разных блоков
- ShouldGetDifferentChecksumsWhileCheckRange: проверяем отличие чексумм разных блоков

#### Негативные тесты
- ShouldCheckRangeWithBrokenBlocks: проверка ошибки "block is broken" через подмену перехваченного внутреннего ответа
- ShouldntCheckRangeWithBigBlockCount: проверка обработки параметра для слишком большого размера блока
