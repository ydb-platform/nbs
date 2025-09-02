import logging
import os
import time

from yatest.common import process

from contrib.ydb.tests.library.harness.daemon import Daemon
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.tasks.test.common.processes import register_process, kill_processes

logger = logging.getLogger()

DEFAULT_CONFIG_TEMPLATE = """
Hostname: "{hostname}"
TasksConfig: <
    TaskPingPeriod: "1s"
    PollForTaskUpdatesPeriod: "1s"
    PollForTasksPeriodMin: "1s"
    PollForTasksPeriodMax: "2s"
    PollForStallingTasksPeriodMin: "2s"
    PollForStallingTasksPeriodMax: "4s"
    TaskStallingTimeout: "5s"
    TaskWaitingTimeout: "3s"
    ScheduleRegularTasksPeriodMin: "2s"
    ScheduleRegularTasksPeriodMax: "4s"
    RunnersCount: 10
    StalkingRunnersCount: 10
    EndedTaskExpirationTimeout: "200s"
    ClearEndedTasksTaskScheduleInterval: "11s"
    ClearEndedTasksLimit: 10
    MaxRetriableErrorCount: 1000
    HangingTaskTimeout: "24h"
    InflightHangingTaskTimeout: "100s"
    StallingHangingTaskTimeout: "30m"
>
PersistenceConfig: <
    Endpoint: "{ydb_endpoint}"
    Database: "{ydb_database}"
    RootPath: "tasks"
>
LoggingConfig: <
    LoggingStderr: <>
    Level: LEVEL_DEBUG
>
"""

SERVICE_NAME = "node"


class Node(Daemon):

    def __init__(self, config_file, working_dir):
        internal_command = yatest_common.binary_path(
            "cloud/tasks/acceptance_tests/recipe/node/node")
        internal_command += " --config " + config_file

        command = [yatest_common.binary_path("cloud/tasks/test/nemesis/nemesis")]
        command += ["--cmd", internal_command]

        super(Node, self).__init__(command=command, cwd=working_dir, timeout=180)


class NodeLauncher:

    def __init__(self, hostname, idx):
        self.__idx = idx

        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)
        config_file = os.path.join(working_dir, "node_config_{}.txt".format(idx))

        self.__config_string = DEFAULT_CONFIG_TEMPLATE.format(
            hostname=hostname,
            ydb_endpoint=os.getenv("YDB_ENDPOINT"),
            ydb_database=os.getenv("YDB_DATABASE")
        )
        with open(config_file, "w") as f:
            f.write(self.__config_string)

        init_database_command = [
            yatest_common.binary_path(
                "cloud/tasks/acceptance_tests/recipe/init-db/init-db"
            ),
            "--config",
            config_file,
        ]

        attempts_left = 20
        while True:
            try:
                process.execute(init_database_command)
                break
            except yatest_common.ExecutionError as e:
                logger.error("init_database_command=%s failed with error=%s", init_database_command, e)

                attempts_left -= 1
                if attempts_left == 0:
                    raise e

                time.sleep(1)
                continue

        self.__node = Node(config_file, working_dir)

    def start(self):
        self.__node.start()
        register_process(SERVICE_NAME, self.__node.daemon.process.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def config_string(self):
        return self.__config_string
