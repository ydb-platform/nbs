import datetime
import logging
import pytest
import threading

from concurrent import futures

import cloud.blockstore.public.sdk.python.protos as protos

from cloud.blockstore.public.sdk.python.client.client import Client
from cloud.blockstore.public.sdk.python.client.session import Session
from cloud.blockstore.public.sdk.python.client.error_codes import EResult
from cloud.blockstore.public.sdk.python.client.error import ClientError


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_logger")


default_disk_id = "path/to/test_volume"
default_mount_token = "mount_token"
default_block_size = 4*1024
default_blocks_count = 1024


class FakeScheduler(object):

    def __init__(self):
        self.__callbacks = []

    def schedule(self, delay, callback):
        pos = len(self.__callbacks)
        self.__callbacks.append(callback)
        return pos

    def cancel(self, pos):
        self.__callbacks[pos] = None

    def cancel_all(self):
        self.__callbacks = []

    def run_all(self):
        callbacks = self.__callbacks
        self.__callbacks = []
        for callback in callbacks:
            if callback is not None:
                callback()

    def has_callbacks(self):
        for callback in self.__callbacks:
            if callback is not None:
                return True

        return False


class FakeClient(object):
    def __init__(self):
        pass


def _test_mount_volume_impl(sync):
    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        response = protos.TMountVolumeResponse(
            SessionId="1",
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def unmount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "1":
            pytest.fail("Wrong session id (expected: 1, actual: {})".format(
                request.SessionId))

        response = protos.TUnmountVolumeResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "unmount_volume_async", unmount_volume_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    if sync:
        session.mount_volume()
        session.unmount_volume()
    else:
        session.mount_volume_async().result()
        session.unmount_volume_async().result()


def _read_write_blocks_test_common(block_size, sync):
    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = block_size
        volume.BlocksCount = default_blocks_count

        response = protos.TMountVolumeResponse(
            SessionId="1",
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def unmount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "1":
            pytest.fail("Wrong session id (expected: 1, actual: {})".format(
                request.SessionId))

        response = protos.TUnmountVolumeResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    def read_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "1":
            pytest.fail("Wrong session id (expected: 1, actual: {})".format(
                request.SessionId))

        # simulate zero response
        buffers = []
        for i in range(0, request.BlocksCount):
            buffer = b"0" * block_size
            buffers.append(buffer)

        response = protos.TReadBlocksResponse(
            Blocks=protos.TIOVector(Buffers=buffers)
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def write_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "1":
            pytest.fail("Wrong session id (expected: 1, actual: {})".format(
                request.SessionId))

        response = protos.TWriteBlocksResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    def zero_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "1":
            pytest.fail("Wrong session id (expected: 1, actual: {})".format(
                request.SessionId))

        response = protos.TZeroBlocksResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "unmount_volume_async", unmount_volume_handler)
    setattr(client_impl, "read_blocks_async", read_blocks_handler)
    setattr(client_impl, "write_blocks_async", write_blocks_handler)
    setattr(client_impl, "zero_blocks_async", zero_blocks_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    if sync:
        session.mount_volume()
        session.read_blocks(0, 1, "")
    else:
        session.mount_volume_async().result()
        session.read_blocks_async(0, 1, "").result()

    buffers = []
    for i in range(0, default_blocks_count):
        buffer = b"0" * block_size
        buffers.append(buffer)

    if sync:
        session.write_blocks(0, buffers)
        session.zero_blocks(0, 1)
        session.read_blocks(0, 1, "")
        session.unmount_volume()
    else:
        session.write_blocks_async(0, buffers).result()
        session.zero_blocks_async(0, 1).result()
        session.read_blocks_async(0, 1, "").result()
        session.unmount_volume_async().result()


def _test_remount_volume_on_invalid_session(sync):

    class ctx:
        session_id = 0

    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        ctx.session_id += 1

        response = protos.TMountVolumeResponse(
            SessionId=str(ctx.session_id),
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def unmount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "2":
            pytest.fail("Wrong session id (expected: 2, actual: {})".format(
                request.SessionId))

        response = protos.TUnmountVolumeResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    def write_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId == "1":
            error = ClientError(EResult.E_INVALID_SESSION.value, "")
            future = futures.Future()
            future.set_exception(error)
            return future

        response = protos.TWriteBlocksResponse()
        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "unmount_volume_async", unmount_volume_handler)
    setattr(client_impl, "write_blocks_async", write_blocks_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    if sync:
        session.mount_volume()
    else:
        session.mount_volume_async().result()

    assert ctx.session_id == 1

    buffers = []
    for i in range(0, default_blocks_count):
        buffer = b"0" * default_block_size
        buffers.append(buffer)

    if sync:
        session.write_blocks(0, buffers)
    else:
        session.write_blocks_async(0, buffers).result()

    assert ctx.session_id == 2

    if sync:
        session.unmount_volume()
    else:
        session.unmount_volume_async().result()


def _test_remount_periodically_to_prevent_unmount_due_to_inactivity(sync):
    timeout = 0.1

    class ctx:
        session_id = 0

    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        ctx.session_id += 1

        response = protos.TMountVolumeResponse(
            SessionId=str(ctx.session_id),
            InactiveClientsTimeout=int(timeout*1000),
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def unmount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        response = protos.TUnmountVolumeResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "unmount_volume_async", unmount_volume_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    if sync:
        session.mount_volume()
    else:
        session.mount_volume_async().result()

    assert ctx.session_id == 1

    scheduler.run_all()

    # run again to ensure remount causes scheduling of another remount
    scheduler.run_all()

    if sync:
        session.unmount_volume()
    else:
        session.unmount_volume_async().result()

    assert ctx.session_id == 3


def _test_stop_automatic_remount_after_unmount(sync):
    timeout = 0.1

    class ctx:
        session_id = 0

    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        ctx.session_id += 1

        response = protos.TMountVolumeResponse(
            SessionId=str(ctx.session_id),
            InactiveClientsTimeout=int(timeout*1000),
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def unmount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        response = protos.TUnmountVolumeResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "unmount_volume_async", unmount_volume_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    if sync:
        session.mount_volume()
    else:
        session.mount_volume_async().result()

    assert ctx.session_id == 1

    scheduler.run_all()

    assert ctx.session_id == 2
    assert scheduler.has_callbacks() is True

    if sync:
        session.unmount_volume()
    else:
        session.unmount_volume_async().result()

    assert scheduler.has_callbacks() is False


def _test_correctly_report_errors_after_remount(sync):

    class ctx:
        session_id = 0

    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        ctx.session_id += 1

        response = protos.TMountVolumeResponse(
            SessionId=str(ctx.session_id),
            Volume=volume
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def unmount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "2":
            pytest.fail("Wrong session id (expected: 2, actual: {})".format(
                request.SessionId))

        response = protos.TUnmountVolumeResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    def write_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId == "1":
            error = ClientError(EResult.E_INVALID_SESSION.value, "")
            future = futures.Future()
            future.set_exception(error)
            return future

        error = ClientError(EResult.E_ARGUMENT.value, "")
        future = futures.Future()
        future.set_exception(error)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "unmount_volume_async", unmount_volume_handler)
    setattr(client_impl, "write_blocks_async", write_blocks_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    if sync:
        session.mount_volume()
    else:
        session.mount_volume_async().result()

    assert ctx.session_id == 1

    buffers = []
    for i in range(0, default_blocks_count):
        buffer = b"0" * default_block_size
        buffers.append(buffer)

    error = None

    try:
        if sync:
            session.write_blocks(0, buffers)
        else:
            session.write_blocks_async(0, buffers).result()
    except ClientError as e:
        error = e

    assert error is not None
    assert error.code == EResult.E_ARGUMENT.value

    if sync:
        session.unmount_volume()
    else:
        session.unmount_volume_async().result()


def _test_reject_read_write_zero_blocks_requests_for_unmounted_volume(sync):
    client_impl = FakeClient()

    def read_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        # simulate zero response
        buffers = []
        for i in range(0, request.BlocksCount):
            buffer = b"0" * default_block_size
            buffers.append(buffer)

        response = protos.TReadBlocksResponse(
            Blocks=protos.TIOVector(Buffers=buffers)
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def write_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        response = protos.TWriteBlocksResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    def zero_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        response = protos.TZeroBlocksResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "read_blocks_async", read_blocks_handler)
    setattr(client_impl, "write_blocks_async", write_blocks_handler)
    setattr(client_impl, "zero_blocks_async", zero_blocks_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    buffers = []
    for i in range(0, default_blocks_count):
        buffer = b"0" * default_block_size
        buffers.append(buffer)

    error = None

    try:
        if sync:
            session.write_blocks(0, buffers)
        else:
            session.write_blocks_async(0, buffers).result()
    except ClientError as e:
        error = e

    assert error is not None
    assert error.code == EResult.E_INVALID_STATE.value

    error = None

    try:
        if sync:
            session.zero_blocks(0, 1)
        else:
            session.zero_blocks_async(0, 1).result()
    except ClientError as e:
        error = e

    assert error is not None
    assert error.code == EResult.E_INVALID_STATE.value

    error = None

    try:
        if sync:
            session.read_blocks(0, 1, "")
        else:
            session.read_blocks_async(0, 1, "").result()
    except ClientError as e:
        error = e

    assert error is not None
    assert error.code == EResult.E_INVALID_STATE.value


def _test_should_try_remount_on_write_following_previous_remount_error(sync):
    timeout = 0.1

    class ctx:
        session_id = 0

    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        ctx.session_id += 1

        if ctx.session_id == 2:
            error = ClientError(EResult.E_INVALID_STATE.value, "")
            future = futures.Future()
            future.set_exception(error)
            return future

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        response = protos.TMountVolumeResponse(
            SessionId=str(ctx.session_id),
            InactiveClientsTimeout=int(timeout*1000),
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def write_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        response = protos.TWriteBlocksResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "write_blocks_async", write_blocks_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    if sync:
        session.mount_volume()
    else:
        session.mount_volume_async().result()

    assert ctx.session_id == 1

    scheduler.run_all()

    # one remount attempt should have been done
    assert ctx.session_id == 2

    # as the second mount attempt was set up to fail,
    # there should be no pending remounts at this point
    assert scheduler.has_callbacks() is False

    buffers = []
    for i in range(0, default_blocks_count):
        buffer = b"0" * default_block_size
        buffers.append(buffer)

    # the next write blocks attempt should cause one more remount and then pass
    if sync:
        session.write_blocks(0, buffers)
    else:
        session.write_blocks_async(0, buffers).result()

    assert ctx.session_id == 3
    assert scheduler.has_callbacks() is True


def test_mount_volume_sync():
    _test_mount_volume_impl(sync=True)


def test_mount_volume_async():
    _test_mount_volume_impl(sync=False)


def test_read_write_blocks_sync():
    _read_write_blocks_test_common(default_block_size, sync=True)


def test_read_write_blocks_async():
    _read_write_blocks_test_common(default_block_size, sync=False)


def test_read_write_blocks_with_non_default_block_size_sync():
    block_size = 512
    # fix me if default block size changes
    assert block_size != default_block_size
    _read_write_blocks_test_common(block_size, sync=True)


def test_read_write_blocks_with_non_default_block_size_async():
    block_size = 512
    # fix me if default block size changes
    assert block_size != default_block_size
    _read_write_blocks_test_common(block_size, sync=False)


def test_remount_volume_in_invalid_session_sync():
    _test_remount_volume_on_invalid_session(sync=True)


def test_remount_volume_in_invalid_session_async():
    _test_remount_volume_on_invalid_session(sync=False)


def test_remount_periodically_to_prevent_unmount_due_to_inactivity_sync():
    _test_remount_periodically_to_prevent_unmount_due_to_inactivity(sync=True)


def test_remount_periodically_to_prevent_unmount_due_to_inactivity_async():
    _test_remount_periodically_to_prevent_unmount_due_to_inactivity(sync=False)


def test_stop_automatic_remount_after_unmount_sync():
    _test_stop_automatic_remount_after_unmount(sync=True)


def test_stop_automatic_remount_after_unmount_async():
    _test_stop_automatic_remount_after_unmount(sync=False)


def test_correctly_report_errors_after_remount_sync():
    _test_correctly_report_errors_after_remount(sync=True)


def test_correctly_report_errors_after_remount_async():
    _test_correctly_report_errors_after_remount(sync=False)


def test_reject_read_write_zero_blocks_requests_for_unmounted_volume_sync():
    _test_reject_read_write_zero_blocks_requests_for_unmounted_volume(sync=True)


def test_reject_read_write_zero_blocks_requests_for_unmounted_volume_async():
    _test_reject_read_write_zero_blocks_requests_for_unmounted_volume(sync=False)


def test_should_try_remount_on_write_following_previous_remount_error_sync():
    _test_should_try_remount_on_write_following_previous_remount_error(sync=True)


def test_should_try_remount_on_write_following_previous_remount_error_async():
    _test_should_try_remount_on_write_following_previous_remount_error(sync=False)


def test_multiple_in_flight_read_write_zero_blocks_requests():

    class ctx:
        in_flight_read_counter = 0
        in_flight_write_counter = 0
        in_flight_zero_counter = 0

        max_in_flight_read_counter = 0
        max_in_flight_write_counter = 0
        max_in_flight_zero_counter = 0

        read_counter_lock = threading.Lock()
        write_counter_lock = threading.Lock()
        zero_counter_lock = threading.Lock()

        complete_request_flag = False
        complete_request_cond_var = threading.Condition()

    request_delay = 0.1

    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        response = protos.TMountVolumeResponse(
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def read_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        with ctx.read_counter_lock:
            ctx.in_flight_read_counter += 1
            if ctx.max_in_flight_read_counter < ctx.in_flight_read_counter:
                ctx.max_in_flight_read_counter = ctx.in_flight_read_counter

        # simulate zero response
        buffers = []
        for i in range(0, request.BlocksCount):
            buffer = b"0" * default_block_size
            buffers.append(buffer)

        response = protos.TReadBlocksResponse(
            Blocks=protos.TIOVector(Buffers=buffers)
        )

        future = futures.Future()

        def send_response():
            ctx.complete_request_cond_var.acquire()
            while not ctx.complete_request_flag:
                ctx.complete_request_cond_var.wait()

            with ctx.read_counter_lock:
                ctx.in_flight_read_counter -= 1

            future.set_result(response)
            ctx.complete_request_cond_var.release()

        threading.Timer(request_delay, send_response).start()
        return future

    def write_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        with ctx.write_counter_lock:
            ctx.in_flight_write_counter += 1
            if ctx.max_in_flight_write_counter < ctx.in_flight_write_counter:
                ctx.max_in_flight_write_counter = ctx.in_flight_write_counter

        response = protos.TWriteBlocksResponse()
        future = futures.Future()

        def send_response():
            ctx.complete_request_cond_var.acquire()
            while not ctx.complete_request_flag:
                ctx.complete_request_cond_var.wait()

            with ctx.write_counter_lock:
                ctx.in_flight_write_counter -= 1

            future.set_result(response)
            ctx.complete_request_cond_var.release()

        threading.Timer(request_delay, send_response).start()
        return future

    def zero_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        with ctx.zero_counter_lock:
            ctx.in_flight_zero_counter += 1
            if ctx.max_in_flight_zero_counter < ctx.in_flight_zero_counter:
                ctx.max_in_flight_zero_counter = ctx.in_flight_zero_counter

        response = protos.TZeroBlocksResponse()
        future = futures.Future()

        def send_response():
            ctx.complete_request_cond_var.acquire()
            while not ctx.complete_request_flag:
                ctx.complete_request_cond_var.wait()

            with ctx.zero_counter_lock:
                ctx.in_flight_zero_counter -= 1

            future.set_result(response)
            ctx.complete_request_cond_var.release()

        threading.Timer(request_delay, send_response).start()
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "read_blocks_async", read_blocks_handler)
    setattr(client_impl, "write_blocks_async", write_blocks_handler)
    setattr(client_impl, "zero_blocks_async", zero_blocks_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    session.mount_volume()

    buffers = []
    for i in range(0, default_blocks_count):
        buffer = b"0" * default_block_size
        buffers.append(buffer)

    num_concurrent_requests = 20

    write_futures = []
    for i in range(0, num_concurrent_requests):
        future = session.write_blocks_async(0, buffers)
        write_futures.append(future)

    zero_futures = []
    for i in range(0, num_concurrent_requests):
        future = session.zero_blocks_async(0, 1)
        zero_futures.append(future)

    read_futures = []
    for i in range(0, num_concurrent_requests):
        future = session.read_blocks_async(0, 1, "")
        read_futures.append(future)

    # allow all requests to proceed
    ctx.complete_request_cond_var.acquire()
    ctx.complete_request_flag = True
    ctx.complete_request_cond_var.notify_all()
    ctx.complete_request_cond_var.release()

    # wait on all futures created so far
    for i in range(0, num_concurrent_requests):
        write_futures[i].result()
        zero_futures[i].result()
        read_futures[i].result()

    assert ctx.max_in_flight_write_counter == num_concurrent_requests
    assert ctx.max_in_flight_zero_counter == num_concurrent_requests
    assert ctx.max_in_flight_read_counter == num_concurrent_requests


def test_use_original_mount_request_headers_for_remount():

    class ctx:
        idempotence_id = "test_idempotence_id"
        trace_id = "test_trace_id"
        request_timeout = int(datetime.timedelta(seconds=30).total_seconds())
        session_id = 0

    client_impl = FakeClient()

    def mount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if idempotence_id != ctx.idempotence_id:
            pytest.fail("Wrong idempotence id (expected: {}, actual: {})".format(
                ctx.idempotence_id,
                idempotence_id))

        if trace_id != ctx.trace_id:
            pytest.fail("Wrong trace id (expected: {}, actual: {})".format(
                ctx.trace_id,
                trace_id))

        if request_timeout != ctx.request_timeout:
            pytest.fail("Wrong request timeout (expected: {}, actual: {})".format(
                str(ctx.request_timeout),
                str(request_timeout)))

        volume = protos.TVolume()
        volume.DiskId = request.DiskId
        volume.BlockSize = default_block_size
        volume.BlocksCount = default_blocks_count

        ctx.session_id += 1

        response = protos.TMountVolumeResponse(
            SessionId=str(ctx.session_id),
            Volume=volume,
        )

        future = futures.Future()
        future.set_result(response)
        return future

    def write_blocks_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId == "1":
            error = ClientError(EResult.E_INVALID_SESSION.value, "")
            future = futures.Future()
            future.set_exception(error)
            return future

        response = protos.TWriteBlocksResponse()
        future = futures.Future()
        future.set_result(response)
        return future

    def unmount_volume_handler(
            request,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout):

        if request.DiskId != default_disk_id:
            pytest.fail("Wrong disk id (expected: {}, actual: {})".format(
                default_disk_id,
                request.DiskId))

        if request.SessionId != "2":
            pytest.fail("Wrong session id (expected: 2, actual: {})".format(
                request.SessionId))

        response = protos.TUnmountVolumeResponse()

        future = futures.Future()
        future.set_result(response)
        return future

    setattr(client_impl, "mount_volume_async", mount_volume_handler)
    setattr(client_impl, "unmount_volume_async", unmount_volume_handler)
    setattr(client_impl, "write_blocks_async", write_blocks_handler)

    scheduler = FakeScheduler()
    client = Client(client_impl)

    session = Session(
        client,
        default_disk_id,
        default_mount_token,
        log=logger,
        scheduler=scheduler,
    )

    session.mount_volume(
        idempotence_id=ctx.idempotence_id,
        trace_id=ctx.trace_id,
        request_timeout=ctx.request_timeout)

    assert ctx.session_id == 1

    buffers = []
    for i in range(0, default_blocks_count):
        buffer = b"0" * default_block_size
        buffers.append(buffer)

    error = None

    try:
        session.write_blocks(0, buffers)
    except ClientError as e:
        error = e

    assert error is None
    assert ctx.session_id == 2

    session.unmount_volume()
