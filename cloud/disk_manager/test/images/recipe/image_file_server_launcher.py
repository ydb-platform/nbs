from pathlib import Path

from cloud.storage.core.tools.common.python.daemon import Daemon
from contrib.ydb.tests.library.harness.kikimr_runner import get_unique_path_for_current_test, ensure_path_exists
import contrib.ydb.tests.library.common.yatest_common as yatest_common

from cloud.tasks.test.common.processes import register_process, kill_processes

SERVICE_NAME = "image_file_server"


class ImageFileServer(Daemon):

    def __init__(self, port, working_dir, image_file_path, other_image_file_path):
        command = [yatest_common.binary_path(
            "cloud/disk_manager/test/images/server/server")]
        command += [
            "start",
            "--image-file-path", image_file_path,
            "--other-image-file-path", other_image_file_path,
            "--port", str(port),
        ]

        super(ImageFileServer, self).__init__(
            commands=[command],
            cwd=working_dir,
            service_name=SERVICE_NAME)


def _check_file_is_valid(path: Path, expected_image_file_size: str):
    if not path.is_file():
        raise RuntimeError(f"Image file path {path} does not exist")
    actual_image_file_size = path.stat().st_size
    if actual_image_file_size != int(expected_image_file_size):
        raise RuntimeError(
            f"Image file size {actual_image_file_size} does not match expected"
        )


class ImageFileServerLauncher:

    def __init__(
            self,
            image_file_path,
            expected_image_file_size,
            other_image_file_path="",
            other_expected_image_file_size=None,
    ):

        self.__image_file_path = Path(image_file_path)
        _check_file_is_valid(expected_image_file_size, image_file_path)
        if other_image_file_path != "":
            self.__other_image_file_path = Path(other_image_file_path)
            if other_expected_image_file_size is None:
                raise RuntimeError(
                    "other_expected_image_file_size can not be None if other_image_file_path is present"
                )
            _check_file_is_valid(self.__other_image_file_path, other_expected_image_file_size)

        self.__port_manager = yatest_common.PortManager()
        self.__port = self.__port_manager.get_port()

        working_dir = get_unique_path_for_current_test(
            output_path=yatest_common.output_path(),
            sub_folder=""
        )
        ensure_path_exists(working_dir)

        self.__daemon = ImageFileServer(
            self.__port,
            working_dir,
            str(self.__image_file_path),
            str(self.__other_image_file_path))

    def start(self):
        self.__daemon.start()
        register_process(SERVICE_NAME, self.__daemon.pid)

    @staticmethod
    def stop():
        kill_processes(SERVICE_NAME)

    @property
    def port(self):
        return self.__port
