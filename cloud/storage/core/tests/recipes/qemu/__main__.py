from library.python.testing.recipe import declare_recipe

from cloud.storage.core.tools.testing.qemu.lib import start, stop

if __name__ == "__main__":
    declare_recipe(start, stop)
