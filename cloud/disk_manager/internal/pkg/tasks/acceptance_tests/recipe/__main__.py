import os

from library.python.testing.recipe import declare_recipe, set_env

from cloud.disk_manager.internal.pkg.tasks.acceptance_tests.recipe.node_launcher import NodeLauncher


NODES_COUNT = 3


def start(argv):
    nodes = []
    for i in range(0, NODES_COUNT):
        nodes.append(NodeLauncher(
            "localhost{}".format(i),
            os.getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
            i
        ))
        nodes[-1].start()

    set_env("DISK_MANAGER_TASKS_ACCEPTANCE_TESTS_RECIPE_NODE0_CONFIG", nodes[0].config_string)


def stop(argv):
    NodeLauncher.stop()


if __name__ == "__main__":
    declare_recipe(start, stop)
