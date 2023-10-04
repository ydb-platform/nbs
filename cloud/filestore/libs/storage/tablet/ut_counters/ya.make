UNITTEST_FOR(cloud/filestore/libs/storage/tablet)

# https://docs.yandex-team.ru/ya-make/manual/common/data#kratkaya-instrukciya-po-ispolzovaniyu-dannyh-v-testah
DATA(ext:ShouldRegisterCountersOnLoad.txt)
DATA(ext:ShouldIncrementAndDecrementSessionCount.txt)
DATA(ext:ShouldCorrectlyWriteThrottlerMaxParams.txt)
DATA(ext:ShouldUpdateValuesOnThrottling.txt)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    tablet_ut_counters.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
