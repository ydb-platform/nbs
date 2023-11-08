# Локальный запуск фазинга

Запустить фазинг можно довольно сложно т.к. тестируемый файл слишком большой для линковшика, с нужными санитайзерами. По этому, для сантайзеров нужно собрать белый лист файлов.

1. Собираем полностью проект в релизной сборке и сохраняем kikimr, который нам нужен без санитайзеров
```
ya make -r ${MOUNT_PATH}/cloud/nbs_internal/blockstore/buildall
mv $(readlink -f ${MOUNT_PATH}/kikimr/driver/kikimr) /tmp/
```

2. Собираем все наши файлы в файл w-list.txt.
```
find ${MOUNT_PATH}/cloud/blockstore -type f \( -name "*.h" \
                  -o -name "*.hpp" \
                  -o -name "*.c" \
                  -o -name "*.cc" \
                  -o -name "*.cpp" \) -exec echo "src:$(pwd)/{}" >> w-list.txt \;
find ${MOUNT_PATH}/cloud/storage -type f \( -name "*.h" \
                  -o -name "*.hpp" \
                  -o -name "*.c" \
                  -o -name "*.cc" \
                  -o -name "*.cpp" \) -exec echo "src:$(pwd)/{}" >> w-list.txt \;
echo "fun:*" >> w-list.txt

```
3. Отключаем любые санитайзеры от kikimr
```
find $MOUNT_PATH/ydb -name ya.make -exec sed -i 's/END()/NO_CLANG_COVERAGE()\nEND()/' {} \;
find $MOUNT_PATH/ydb -name ya.make -exec sed -i 's/END()/NO_SANITIZE()\nEND()/' {} \;
```

4. Заходим в папку fuzzing и собираем проект fuzzing
```
ya make -A -r --sanitize=address \
              --sanitize-coverage=trace-div,trace-gep \
              --sanitizer-flag=-fsanitize=fuzzer \
              --sanitizer-flag=-fsanitize-coverage-allowlist=$MOUNT_PATH/w-list.txt \
              --clang-coverage \
              --build-all
```

5. Далее это можно запустить, как обычную программу с параметрами libFuzzer библиотеки https://llvm.org/docs/LibFuzzer.html. Однако для работы ему потребуется kikimr и файлы конфигурации. Поэтому копируем собранный фазер в папку, где лежат конфигурации и запускаем kikimr
```
mv fuzzing ${MOUNT_PATH}/cloud/blockstore/bin
cd ${MOUNT_PATH}/cloud/blockstore/bin

./setup.sh
./kikimr-configure.sh
./kikimr-format.sh
/tmp/kikimr server \
    --tcp \
    --node              1 \
    --grpc-port         9001 \
    --mon-port          8765 \
    --bootstrap-file    ${MOUNT_PATH}/cloud/blockstore/bin/static/boot.txt \
    --bs-file           ${MOUNT_PATH}/cloud/blockstore/bin/static/bs.txt \
    --channels-file     ${MOUNT_PATH}/cloud/blockstore/bin/static/channels.txt \
    --domains-file      ${MOUNT_PATH}/cloud/blockstore/bin/static/domains.txt \
    --log-file          ${MOUNT_PATH}/cloud/blockstore/bin/static/log.txt \
    --naming-file       ${MOUNT_PATH}/cloud/blockstore/bin/static/names.txt \
    --sys-file          ${MOUNT_PATH}/cloud/blockstore/bin/static/sys.txt \
    --ic-file           ${MOUNT_PATH}/cloud/blockstore/bin/static/ic.txt \
    --vdisk-file        ${MOUNT_PATH}/cloud/blockstore/bin/static/vdisks.txt \
    >${OUT_PATH}/kikimr-server.log 2>&1 &
sleep 60
./kikimr-init.sh

sed -i 's/NbdEnabled: true/NbdEnabled: true\n  VhostEnabled: true/' nbs/nbs-server.txt
```

5. Последнее, что надо сделать перед запуском, это настроить сбор coverage
```
export LLVM_PROFILE_FILE="fuzz-%p.profraw"
export ASAN_OPTIONS="exitcode=100:coverage=1:allocator_may_return_null=1"
export ASAN_SYMBOLIZER_PATH=$(ya tool llvm-symbolizer --print-path)
```

6. Для простоты запуска подготовлен скрипт run_fuzzing.py у которого есть опция, необходимая для SDL - он может парсить вывод фазера и проверять обнаружены новые пути или нет за определенное время.

```
python3 ${MOUNT_PATH}/cloud/storage/core/tools/fuzzing/run_fuzzing.py \
        --exec ${MOUNT_PATH}/cloud/blockstore/bin/fuzzing \
        --total_time 100 \
        --jobs 30 \
        --corpus_path ${NEW_CORPUS_PATH} \
        --white_list $MOUNT_PATH/white-list.txt \
        --stop_time 1000 2> /dev/null
```
Однако запускать можно и руками, согласно параметрам библиотеки libFuzzer

7. Построение отчета coverage
```
export LVM_PROFDATA=$(ya tool llvm-profdata --print-path)
export LVM_COV=$(ya tool llvm-cov --print-path)

$LVM_PROFDATA merge -sparse fuzz-*.profraw -o fuzz.profdata
$LVM_COV report $(pwd)/fuzzing -instr-profile=fuzz.profdata > ${OUT_PATH}/coverage_report.txt
```
