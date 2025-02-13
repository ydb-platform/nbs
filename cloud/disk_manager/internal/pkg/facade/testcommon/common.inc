SET_APPEND(RECIPE_ARGS --multiple-disk-managers)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(4)

SIZE(LARGE)
TAG(ya:fat ya:force_sandbox ya:sandbox_coverage sb:ssd)

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_multi_slots.inc)

REQUIREMENTS(
    cpu:8
    ram:24
    disk_usage:200
)
