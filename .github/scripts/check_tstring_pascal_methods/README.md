# TString PascalCase Methods Check

Проверка запрещает PascalCase-методы `Size()`, `Data()` и `Empty()` у строковых
типов из `util/generic/string.h` (`TString`, `TStringBuf`, `TUtf16String*`,
`TWtringBuf`, `std::string` и т.п.).

В новых версиях contrib эти методы у `TString` удалены — нужно использовать
`size()`, `data()` и `empty()`.

Скрипт смотрит только на **добавленные строки** в diff. Вызовы вроде
`range.Size()` у других типов не трогаются.

Скрипт: `.github/scripts/check_tstring_pascal_methods.py`  
Тесты: `.github/scripts/tests/check_tstring_pascal_methods_test.py`  
CI: workflow `.github/workflows/check-tstring-pascal-methods.yaml` (на PR в `main`)

## Юнит-тесты

Из корня репозитория:

```bash
python3 .github/scripts/tests/check_tstring_pascal_methods_test.py -v
```

## Проверка как в CI

То же, что делает GitHub Actions на pull request — diff от `main` до текущего
`HEAD`:

```bash
git fetch origin main

python3 .github/scripts/check_tstring_pascal_methods.py \
  --from-ref origin/main \
  --to-ref HEAD
```

Код выхода `0` — нарушений нет, `1` — найдены PascalCase-вызовы на строках.

## Проверка staged-изменений

Перед коммитом, только по файлам в индексе:

```bash
git add path/to/file.cpp

python3 .github/scripts/check_tstring_pascal_methods.py --cached
```

## Проверка конкретных файлов

```bash
python3 .github/scripts/check_tstring_pascal_methods.py \
  --from-ref origin/main \
  --to-ref HEAD \
  cloud/filestore/libs/storage/model/block_buffer_ut.cpp
```

## Ручная проверка «должно упасть»

```bash
sed -i 's/Data\.size()/Data.Size()/' cloud/filestore/libs/storage/model/block_buffer_ut.cpp
git add cloud/filestore/libs/storage/model/block_buffer_ut.cpp

python3 .github/scripts/check_tstring_pascal_methods.py --cached

# откат
git restore --staged cloud/filestore/libs/storage/model/block_buffer_ut.cpp
git restore cloud/filestore/libs/storage/model/block_buffer_ut.cpp
```

Ожидаемый вывод:

```text
use .size() instead of .Size()
```

## Что проверяется / что нет

| Пример | Результат |
|--------|-----------|
| `TString s; s.Size()` в новой строке | fail |
| `TStringBuf buf; buf.Data()` в новой строке | fail |
| `TRange range; range.Size()` | ok |
| Старый код без изменений в diff | ok |

Ограничение: `auto s = GetString(); s.Size()` не поймается, если `s` не
объявлен явно как строковый тип в том же файле.
