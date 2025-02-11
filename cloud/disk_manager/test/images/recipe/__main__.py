import os

from yatest.common import process

from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common
from library.python.testing.recipe import declare_recipe, set_env

from cloud.disk_manager.test.images.recipe.image_file_server_launcher import ImageFileServerLauncher
from cloud.disk_manager.test.images.recipe.vmdk_image_generator import VMDKImageGenerator
from cloud.disk_manager.test.images.recipe.raw_image_generator import RawImageGenerator


def start(argv):
    raw_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/raw.img")
    raw_image_file_size = 67108864
    raw_image_file_server = ImageFileServerLauncher(raw_image_file_path, raw_image_file_size)
    raw_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_RAW_IMAGE_FILE_SERVER_PORT", str(raw_image_file_server.port))
    set_env("DISK_MANAGER_RECIPE_RAW_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_RAW_IMAGE_CRC32", "3776401828")

    qcow2_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2.img")
    other_qcow2_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2_other.img")
    qcow2_image_file_size = 16338944
    other_qcow2_image_file_size = 33882112
    qcow2_image_file_server = ImageFileServerLauncher(
        qcow2_image_file_path,
        qcow2_image_file_size,
        other_qcow2_image_file_path,
        other_qcow2_image_file_size,
    )
    qcow2_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_QCOW2_IMAGE_FILE_SERVER_PORT", str(qcow2_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_QCOW2_IMAGE_SIZE", "117440512")
    set_env("DISK_MANAGER_RECIPE_QCOW2_IMAGE_CRC32", "1813398331")
    set_env("DISK_MANAGER_RECIPE_OTHER_QCOW2_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_OTHER_QCOW2_IMAGE_CRC32", "3837173913")

    vmdk_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vmdk.img")
    vmdk_image_file_size = 35782656
    vmdk_image_file_server = ImageFileServerLauncher(vmdk_image_file_path, vmdk_image_file_size)
    vmdk_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_VMDK_IMAGE_FILE_SERVER_PORT", str(vmdk_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_VMDK_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_VMDK_IMAGE_CRC32", "1412309815")

    vmdk_stream_optimized_file_path = yatest_common.source_path(
        "cloud/disk_manager/test/images/recipe/data/vmdk_stream_optimized.img",
    )
    vmdk_stream_optimized_file_size = 34455040
    vmdk_stream_optimized_image_file_server = ImageFileServerLauncher(
        vmdk_stream_optimized_file_path,
        vmdk_stream_optimized_file_size,
    )
    vmdk_stream_optimized_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_FILE_SERVER_PORT", str(vmdk_stream_optimized_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_SIZE", "67108864")
    set_env("DISK_MANAGER_RECIPE_VMDK_STREAM_OPTIMIZED_IMAGE_CRC32", "1412309815")

    vhd_raw_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vhd_raw.img")
    vhd_raw_image_file_size = 67125760
    vhd_raw_image_file_server = ImageFileServerLauncher(
        vhd_raw_image_file_path,
        vhd_raw_image_file_size,
    )
    vhd_raw_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_VHD_RAW_IMAGE_FILE_SERVER_PORT", str(vhd_raw_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_VHD_RAW_IMAGE_SIZE", "67125760")
    set_env("DISK_MANAGER_RECIPE_VHD_RAW_IMAGE_CRC32", "1296862566")
    image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vhd_raw_image_map.json")
    set_env("DISK_MANAGER_RECIPE_VHD_RAW_IMAGE_MAP_FILE", image_map_file_path)

    vhd_dynamic_image_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vhd_dynamic.img")
    vhd_dynamic_image_file_size = 54541824
    vhd_dynamic_image_file_server = ImageFileServerLauncher(vhd_dynamic_image_file_path, vhd_dynamic_image_file_size)
    vhd_dynamic_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_FILE_SERVER_PORT", str(vhd_dynamic_image_file_server.port))
    # size and crc32 after converting to raw image
    set_env("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_SIZE", "117469184")
    set_env("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_CRC32", "4215190084")
    image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vhd_dynamic_image_map.json")
    set_env("DISK_MANAGER_RECIPE_VHD_DYNAMIC_IMAGE_MAP_FILE", image_map_file_path)

    vhd_ubuntu1604_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/vhd_images/ubuntu1604-ci-stable")
    if os.path.exists(vhd_ubuntu1604_image_file_path):
        vhd_ubuntu1604_image_file_size = 4306535424
        vhd_ubuntu1604_image_file_server = ImageFileServerLauncher(
            vhd_ubuntu1604_image_file_path,
            vhd_ubuntu1604_image_file_size,
        )
        vhd_ubuntu1604_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_FILE_SERVER_PORT", str(vhd_ubuntu1604_image_file_server.port))
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_SIZE", "15246508032")
        set_env("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_CRC32", "3937858947")
        image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vhd_ubuntu1604_image_map.json")
        set_env("DISK_MANAGER_RECIPE_VHD_UBUNTU1604_IMAGE_MAP_FILE", image_map_file_path)

    nonexistent_image_file_server = ImageFileServerLauncher("nonexistent", None)
    nonexistent_image_file_server.start()
    set_env("DISK_MANAGER_RECIPE_NON_EXISTENT_IMAGE_FILE_SERVER_PORT", str(nonexistent_image_file_server.port))

    ubuntu1804_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/qcow2_images/ubuntu-18.04-minimal-cloudimg-amd64.img")
    if os.path.exists(ubuntu1804_image_file_path):
        ubuntu1804_image_file_size = 332595200
        ubuntu1804_image_file_server = ImageFileServerLauncher(
            ubuntu1804_image_file_path,
            ubuntu1804_image_file_size,
        )
        ubuntu1804_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_FILE_SERVER_PORT", str(ubuntu1804_image_file_server.port))
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_SIZE", "2361393152")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_CRC32", "2577917554")
        image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2_ubuntu1804_image_map.json")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1804_IMAGE_MAP_FILE", image_map_file_path)

    ubuntu1604_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/qcow2_images/ubuntu1604-ci-stable")
    if os.path.exists(ubuntu1604_image_file_path):
        ubuntu1604_image_file_size = 4162256896
        ubuntu1604_image_file_server = ImageFileServerLauncher(
            ubuntu1604_image_file_path,
            ubuntu1604_image_file_size,
        )
        ubuntu1604_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_FILE_SERVER_PORT", str(ubuntu1604_image_file_server.port))
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_SIZE", "15246508032")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_CRC32", "3937858947")
        image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/qcow2_ubuntu1604_image_map.json")
        set_env("DISK_MANAGER_RECIPE_QCOW2_UBUNTU1604_IMAGE_MAP_FILE", image_map_file_path)

    ubuntu2204_vmdk_image_file_path = yatest_common.build_path("cloud/disk_manager/test/images/resources/vmdk_images/ubuntu-22.04-jammy-server-cloudimg-amd64.vmdk")
    if os.path.exists(ubuntu2204_vmdk_image_file_path):
        ubuntu2204_vmdk_image_file_size = 667248128
        ubuntu2204_vmdk_image_file_server = ImageFileServerLauncher(
            ubuntu2204_vmdk_image_file_path,
            ubuntu2204_vmdk_image_file_size,
        )
        ubuntu2204_vmdk_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_FILE_SERVER_PORT", str(ubuntu2204_vmdk_image_file_server.port))
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_SIZE", "10737418240")
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_CRC32", "3896929631")
        image_map_file_path = yatest_common.source_path("cloud/disk_manager/test/images/recipe/data/vmdk_ubuntu2204_image_map.json")
        set_env("DISK_MANAGER_RECIPE_VMDK_UBUNTU2204_IMAGE_MAP_FILE", image_map_file_path)

    windows_vmdk_image_file_path = yatest_common.build_path(
        "cloud/disk_manager/test/images/resources/vmdk_images/windows-vmdk-stream-optimised-multiple-grains.vmdk")
    if os.path.exists(windows_vmdk_image_file_path):
        windows_vmdk_image_file_size = 1354152960
        windows_vmdk_image_file_server = ImageFileServerLauncher(
            windows_vmdk_image_file_path,
            windows_vmdk_image_file_size,
        )
        windows_vmdk_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_VMDK_WINDOWS_FILE_SERVER_PORT", str(windows_vmdk_image_file_server.port))
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_VMDK_WINDOWS_IMAGE_SIZE", "8589934592")
        set_env("DISK_MANAGER_RECIPE_VMDK_WINDOWS_IMAGE_CRC32", "2831814743")
        image_map_file_path = yatest_common.source_path(
            "cloud/disk_manager/test/images/recipe/data/windows_vmdk_stream_optimised_multiple_grains_image_map.json",
        )
        set_env("DISK_MANAGER_RECIPE_VMDK_WINDOWS_IMAGE_MAP_FILE", image_map_file_path)

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
    invalid_qcow2_image_file_server = ImageFileServerLauncher(invalid_qcow2_image_file_path, None)
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
    qcow2_fuzzing_image_file_server = ImageFileServerLauncher(qcow2_fuzzing_image_file_path, None)
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

        generated_vmdk_image_file_server = ImageFileServerLauncher(generated_vmdk_image_file_path, None)
        generated_vmdk_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_FILE_SERVER_PORT", str(generated_vmdk_image_file_server.port))
        # size and crc32 after converting to raw image
        set_env("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_SIZE", str(vmdk_image_generator.raw_image_size))
        set_env("DISK_MANAGER_RECIPE_GENERATED_VMDK_IMAGE_CRC32", str(vmdk_image_generator.raw_image_crc32))

    if '--generate-big-raw-images' in argv:
        big_raw_image_file_path = os.path.join(working_dir, "generated_big_raw_image")
        other_big_raw_image_file_path = os.path.join(working_dir, "generated_other_big_raw_image")
        big_raw_image_file_size = 536870912  # 512 MiB
        other_big_raw_image_file_size = 1073741824  # 1 GiB
        raw_image_generator = RawImageGenerator(big_raw_image_file_path, big_raw_image_file_size)
        raw_image_generator.generate()
        other_raw_image_generator = RawImageGenerator(other_big_raw_image_file_path, other_big_raw_image_file_size)
        other_raw_image_generator.generate()
        big_raw_image_file_server = ImageFileServerLauncher(
            big_raw_image_file_path,
            big_raw_image_file_size,
            other_big_raw_image_file_path,
            other_big_raw_image_file_size,
        )
        big_raw_image_file_server.start()
        set_env("DISK_MANAGER_RECIPE_BIG_RAW_IMAGE_FILE_SERVER_PORT", str(big_raw_image_file_server.port))
        set_env("DISK_MANAGER_RECIPE_BIG_RAW_IMAGE_SIZE", str(big_raw_image_file_size))
        set_env("DISK_MANAGER_RECIPE_BIG_RAW_IMAGE_CRC32", str(raw_image_generator.image_crc32))
        set_env("DISK_MANAGER_RECIPE_OTHER_BIG_RAW_IMAGE_SIZE", str(other_big_raw_image_file_size))
        set_env("DISK_MANAGER_RECIPE_OTHER_BIG_RAW_IMAGE_CRC32", str(other_raw_image_generator.image_crc32))


def stop(argv):
    ImageFileServerLauncher.stop()


if __name__ == "__main__":
    declare_recipe(start, stop)
