# Локальный запуск фазинга

Запустить фазинг можно 2 способами:
1. Через ya make
2. Как обычный бинарник
Разница в том, что при запуске через ya make часть параметров libfuzzer обрезается. Так что для более тонкой настройки нужен 2 способ запуска.

## Запуск через ya make
Для локального запуска фазинга на 600 секунд можно вызвать команду:
```
ya make -A -r --sanitize=address --sanitizer-flag=-fsanitize=fuzzer --sanitize-coverage=trace-div,trace-gep --fuzzing --fuzz-local-store --fuzz-opts="-max_total_time=600"
```

## Запуск через бинарник
Для сборки программы используется команда:
```
ya make -r --sanitize=address --sanitizer-flag=-fsanitize=fuzzer --sanitize-coverage=trace-div,trace-gep
```

Далее это можно запустить, как обычную программу с параметрами libFuzzer библиотеки https://llvm.org/docs/LibFuzzer.html
Но для простоты запуска подготовлен скрипт run_fuzzing.py у которого есть опция необходимая для SDL - он может парсить вывод фазера и проверять обнаружены новые пути или нет за определенное время.

```
mkdir /tmp/corpus -p

export ASAN_OPTIONS="exitcode=100:coverage=1:allocator_may_return_null=1"
export ASAN_SYMBOLIZER_PATH=$(ya tool llvm-symbolizer --print-path)

python3 cloud/storage/core/tools/fuzzing/run_fuzzing.py --exec ./fuzz             \
                                                        --total_time 100          \
                                                        --jobs 1                  \
                                                        --corpus_path /tmp/corpus \
                                                        --stop_time 100           \
                                                        --max_length  512
```

Если по описанию `stack trace` не получается восстановить причиную ошибки, например из-за непрогруженных символов, попробуйте собрать `fuzz` без ключика `-r`
