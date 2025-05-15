SET_APPEND(RECIPE_ARGS --multiple-disk-managers)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

SIZE(LARGE)
TAG(ya:fat ya:force_sandbox ya:sandbox_coverage sb:ssd)

IF (ARCADIA_SANDBOX_SINGLESLOT)
    TAG(ya:large_tests_on_single_slots)
ENDIF()

REQUIREMENTS(
    cpu:8
    ram:24
    disk_usage:200
)
