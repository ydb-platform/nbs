LIBRARY()

SRCS(
    behaviour.cpp
    executor.cpp
    interrupt.cpp
    task_executor_controller.cpp
    executor_controller.cpp
    task_executor.cpp
    finish_task.cpp
    assign_tasks.cpp
    fetch_tasks.cpp
    config.cpp
    add_tasks.cpp
    task_enabled.cpp
    lock_pinger.cpp
    initialization.cpp
)

PEERDIR(
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/services/metadata/initializer
    contrib/ydb/core/base
    contrib/ydb/services/metadata/request
)

END()
