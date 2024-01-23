import os

from yatest.common import process

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common
from library.python.testing.recipe import declare_recipe, set_env

from cloud.disk_manager.test.images.recipe.image_file_server_launcher import ImageFileServerLauncher
from cloud.disk_manager.test.images.recipe.vmdk_image_generator import VMDKImageGenerator


def start(argv):
    raw_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/raw.img")
    raw_image_file_server = ImageFileServerLauncher(raw_image_file_path)
    raw_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_RAW_IMAGE_FILE_SERVER_PORT", str(raw_image_file_server.port))
    set_env("DISK_MANAGER_RECIPE_RAW_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_RAW_IMAGE_CRC32", "3776401828")

    qcow2_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2.img")
    other_qcow2_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2_other.img")
    qcow2_image_file_server = ImageFileServerLauncher(qcow2_image_file_path, other_qcow2_image_file_path)
    qcow2_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_QCOW2_IMAGE_FILE_SERVER_PORT", str(qcow2_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_QCOW2_IMAGE_SIZE", "117440512")
    set_env("DISK_MANAGER_RECIPE_QCOW2_IMAGE_CRC32", "1813398331")
    set_env("DISK_MANAGER_RECIPE_OTHER_QCOW2_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_OTHER_QCOW2_IMAGE_CRC32", "3837173913")

    vmdk_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vmdk.img")
    vmdk_image_file_server = ImageFileServerLauncher(vmdk_image_file_path)
    vmdk_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_VMDK_IMAGE_FILE_SERVER_PORT", str(vmdk_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_VMDK_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_VMDK_IMAGE_CRC32", "1412309815")

    vmdk_stream_optimized_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vmdk_stream_optimized.img")
    vmdk_stream_optimized_image_file_server = ImageFileServerLauncher(vmdk_stream_optimized_file_path)
    vmdk_stream_optimized_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_FILE_SERVER_PORT", str(vmdk_stream_optimized_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_CRC32", "1412309815")

    nonexistent_image_file_server = ImageFileServerLauncher("nonexistent")
    nonexistent_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_NON_EXISTENT_IMAGE_FILE_SERVER_PORT", str(nonexistent_image_file_server.port))

    ubuntu1804_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/qcow2_images/ubuntu-18.04-minimal-cloudimg-amd64.img")
    if os.path.exists(ubuntu1804_image_file_path):
        ubuntu1804_image_file_server = ImageFileServerLauncher(ubuntu1804_image_file_path)
        ubuntu1804_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_FILE_SERVER_PORT", str(ubuntu1804_image_file_server.port))
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_FILE_SIZE", "332595200")
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_SIZE", "2361393152")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_CRC32", "2577917554")
        image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2_ubuntu1804_image_map.json")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_MAP_FILE", image_map_file_path)

    ubuntu1604_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/qcow2_images/ubuntu1604-ci-stable")
    if os.path.exists(ubuntu1604_image_file_path):
        ubuntu1604_image_file_server = ImageFileServerLauncher(ubuntu1604_image_file_path)
        ubuntu1604_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_FILE_SERVER_PORT", str(ubuntu1604_image_file_server.port))
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_FILE_SIZE", "332595200")
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_SIZE", "15246295040")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_CRC32", "1189208160")
        image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2_ubuntu1604_image_map.json")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_MAP_FILE", image_map_file_path)

    ubuntu2204_vmdk_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/vmdk_images/ubuntu-22.04-jammy-server-cloudimg-amd64.vmdk")
    if os.path.exists(ubuntu2204_vmdk_image_file_path):
        ubuntu2204_vmdk_image_file_server = ImageFileServerLauncher(ubuntu2204_vmdk_image_file_path)
        ubuntu2204_vmdk_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_FILE_SERVER_PORT", str(ubuntu2204_vmdk_image_file_server.port))
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_FILE_SIZE", "667248128")
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_SIZE", "10737418240")
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_CRC32", "3896929631")
        image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vmdk_ubuntu2204_image_map.json")
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_MAP_FILE", image_map_file_path)

    # reproduces panic issue (NBS-4635)
    qcow2_panic_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/qcow2_images/panic.img")
    if os.path.exists(qcow2_panic_image_file_path):
        qcow2_panic_image_file_server = ImageFileServerLauncher(qcow2_panic_image_file_path)
        qcow2_panic_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_QCOW2_PANIC_IMAGE_FILE_SERVER_PORT", str(qcow2_panic_image_file_server.port))
        set_env("DISK_MANAGER_RECIPE_QCOW2_PANIC_IMAGE_FILE_SIZE", "7348420608")
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_QCOW2_PANIC_IMAGE_SIZE", "21474836480")
        set_env("DISK_MANAGER_RECIPE_QCOW2_PANIC_IMAGE_CRC32", "3101932729")

    working_dir = get_unique_path_for_current_test(
        output_path=yatest_common.output_path(),
        sub_folder=""
    )
    ensure_path_exists(working_dir)

    invalid_qcow2_image_file_path = os.path.join(
        working_dir,
        "invalid_qcow2_image"
    )
    process.execute([
        yatest_common.binary_path(
            "cloud/disk_manager/test/images/qcow2generator/qcow2generator"
        ),
        "--mode",
        "invalid",
        "--output-file-path",
        invalid_qcow2_image_file_path,
    ])
    invalid_qcow2_image_file_server = ImageFileServerLauncher(invalid_qcow2_image_file_path)
    invalid_qcow2_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_INVALID_QCOW2_IMAGE_FILE_SERVER_PORT", str(invalid_qcow2_image_file_server.port))

    qcow2_fuzzing_image_file_path = os.path.join(
        working_dir,
        "qcow2_fuzzing_image"
    )
    process.execute([
        yatest_common.binary_path(
            "cloud/disk_manager/test/images/qcow2generator/qcow2generator"
        ),
        "--mode",
        "fuzzing",
        "--output-file-path",
        qcow2_fuzzing_image_file_path,
    ])
    qcow2_fuzzing_image_file_server = ImageFileServerLauncher(qcow2_fuzzing_image_file_path)
    qcow2_fuzzing_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_QCOW2_FUZZING_IMAGE_FILE_SERVER_PORT", str(qcow2_fuzzing_image_file_server.port))

    if '--generate-vmdk-image' in argv:
        generated_raw_image_file_path = os.path.join(
            working_dir,
            "generated_raw_image",
        )
        generated_vmdk_image_file_path = os.path.join(
            working_dir,
            "generated_vmdk_image"
        )
        vmdk_image_generator = VMDKImageGenerator(generated_raw_image_file_path, generated_vmdk_image_file_path)
        vmdk_image_generator.generate()

        generated_vmdk_image_file_server = ImageFileServerLauncher(generated_vmdk_image_file_path)
        generated_vmdk_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_FILE_SERVER_PORT", str(generated_vmdk_image_file_server.port))
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_SIZE", str(vmdk_image_generator.raw_image_size))
        set_env("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_CRC32", str(vmdk_image_generator.raw_image_crc32))


def stop(argv):
    ImageFileServerLauncher.stop()


if __name__ == "__main__":
    declare_recipe(start, stop)
