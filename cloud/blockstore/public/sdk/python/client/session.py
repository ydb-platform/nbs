import copy
import enum
import logging
import threading

from concurrent import futures
from datetime import datetime
from logging import Logger
from typing import Callable

import cloud.blockstore.public.sdk.python.protos as protos

from .client import SessionInfo
from .error import ClientError, _handle_errors
from .error_codes import EResult
from .scheduler import Scheduler


class _EMountState(enum.Enum):
    UNINITIALIZED = 0,
    MOUNT_COMPLETED = 1,
    MOUNT_REQUESTED = 2,
    MOUNT_IN_PROGRESS = 3,
    UNMOUNT_IN_PROGRESS = 4


class Session(object):

    def __init__(
            self,
            client,
            disk_id: str,
            mount_token: str,
            access_mode: protos.EVolumeAccessMode = protos.EVolumeAccessMode.Value("VOLUME_ACCESS_READ_WRITE"),
            mount_mode: protos.EVolumeAccessMode = protos.EVolumeMountMode.Value("VOLUME_MOUNT_LOCAL"),
            throttling_disabled: bool = False,
            mount_seq_number: int = 0,
            log: Logger | None = None,
            scheduler : Scheduler | None = None):

        self.__client = client
        self.__disk_id = disk_id
        self.__mount_token = mount_token
        self.__access_mode = access_mode
        self.__mount_mode = mount_mode
        self.__throttling_disabled = throttling_disabled
        self.__mount_seq_number = mount_seq_number

        self.__mount_lock = threading.Lock()

        self.__state = _EMountState.UNINITIALIZED
        self.__volume: protos.TVolume | None = None
        self.__session_info: SessionInfo | None = None

        self.__mount_headers = None

        if scheduler is None:
            self.__scheduler = Scheduler()
        else:
            self.__scheduler = scheduler

        if log is not None:
            self.log = log
        else:
            self.log = logging.getLogger("session")

    @property
    def disk_id(self) -> str:
        return self.__disk_id

    @property
    def access_mode(self) -> protos.EVolumeAccessMode:
        return self.__access_mode

    @property
    def mount_mode(self) -> protos.EVolumeMountMode:
        return self.__mount_mode

    @property
    def throttling_disabled(self) -> bool:
        return self.__throttling_disabled

    @property
    def client(self):
        return self.__client

    @property
    def volume(self) -> protos.TVolume:
        with self.__mount_lock:
            if self.__volume is None:
                return None
            return copy.deepcopy(self.__volume)

    @property
    def info(self) -> SessionInfo | None:
        with self.__mount_lock:
            if self.__session_info is None:
                return None
            return copy.deepcopy(self.__session_info)

    @_handle_errors
    def mount_volume_async(
            self,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None,
            encryption_spec: protos.TEncryptionSpec | None = None) -> futures.Future:

        result = futures.Future()

        with self.__mount_lock:
            if self.__state == _EMountState.MOUNT_IN_PROGRESS:
                error = ClientError(
                    EResult.E_INVALID_STATE.value,
                    "Volume is already being mounted")
                result.set_exception(error)
                return result

            if self.__state == _EMountState.UNMOUNT_IN_PROGRESS:
                error = ClientError(
                    EResult.E_INVALID_STATE.value,
                    "Volume is being unmounted")
                result.set_exception(error)
                return result

            self.__mount_headers = protos.THeaders(
                IdempotenceId=idempotence_id,
                TraceId=trace_id,
                RequestTimeout=request_timeout,
            )

            self.__state = _EMountState.MOUNT_IN_PROGRESS

        self.log.debug("Submit MountVolume request")

        future = self.__client.mount_volume_async(
            self.__disk_id,
            self.__mount_token,
            self.__access_mode,
            self.__mount_mode,
            self.__throttling_disabled,
            self.__mount_seq_number,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout,
            encryption_spec)

        def process_mount_response(f):
            with self.__mount_lock:
                try:
                    r = f.result()
                    self._process_mount_response(r["Volume"], r["SessionInfo"])
                    result.set_result(r)
                except ClientError as e:
                    self.log.error("MountVolume request failed with error: %s", e)
                    self.__state = _EMountState.MOUNT_REQUESTED
                    result.set_exception(e)

        future.add_done_callback(process_mount_response)
        return result

    @_handle_errors
    def mount_volume(
            self,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None,
            encryption_spec: protos.TEncryptionSpec | None = None) -> dict:

        return self.mount_volume_async(
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout,
            encryption_spec).result()

    @_handle_errors
    def unmount_volume_async(
            self,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        session_id = ""
        result = futures.Future()

        with self.__mount_lock:
            if self.__state == _EMountState.UNINITIALIZED:
                error = ClientError(
                    EResult.E_INVALID_STATE.value,
                    "Volume is not mounted")
                result.set_exception(error)
                return result

            if self.__state == _EMountState.UNMOUNT_IN_PROGRESS:
                error = ClientError(
                    EResult.E_INVALID_STATE.value,
                    "Volume is already being unmounted")
                result.set_exception(error)
                return result

            if self.__state == _EMountState.MOUNT_IN_PROGRESS:
                error = ClientError(
                    EResult.E_INVALID_STATE.value,
                    "Volume is being mounted")
                result.set_exception(error)
                return result

            self.__state = _EMountState.UNMOUNT_IN_PROGRESS

            if self.__session_info is not None:
                session_id = self.__session_info.session_id

        self.log.debug("Submit UnmountVolume request")

        future = self.__client.unmount_volume_async(
            self.__disk_id,
            session_id,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout)

        def process_response(f):
            try:
                self.log.debug("Complete UnmountVolume request")
                response = f.result()
                with self.__mount_lock:
                    self._reset_state()
                result.set_result(response)
            except ClientError as e:
                self.log.error("UnmountVolume request failed with error: %s", e)
                with self.__mount_lock:
                    self.__state = _EMountState.MOUNT_COMPLETED
                result.set_exception(e)

        future.add_done_callback(process_response)
        return result

    @_handle_errors
    def unmount_volume(
            self,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> dict:

        return self.unmount_volume_async(
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).result()

    @_handle_errors
    def read_blocks_async(
            self,
            start_index: int,
            blocks_count: int,
            checkpoint_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        def read_blocks_impl(session_id):
            return self.__client.read_blocks_async(
                self.__disk_id,
                start_index,
                blocks_count,
                checkpoint_id,
                session_id,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)

        future = futures.Future()
        self._process_request(future, read_blocks_impl, "ReadBlocks")
        return future

    @_handle_errors
    def read_blocks(
            self,
            start_index: int,
            blocks_count: int,
            checkpoint_id: str,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> list[bytes]:

        return self.read_blocks_async(
            start_index,
            blocks_count,
            checkpoint_id,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).result()

    @_handle_errors
    def write_blocks_async(
            self,
            start_index: int,
            blocks: bytes,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        def write_blocks_impl(session_id):
            return self.__client.write_blocks_async(
                self.__disk_id,
                start_index,
                blocks,
                session_id,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)

        future = futures.Future()
        self._process_request(future, write_blocks_impl, "WriteBlocks")
        return future

    @_handle_errors
    def write_blocks(
            self,
            start_index: int,
            blocks: bytes,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        return self.write_blocks_async(
            start_index,
            blocks,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).result()

    @_handle_errors
    def zero_blocks_async(
            self,
            start_index: int,
            blocks_count: int,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None) -> futures.Future:

        def zero_blocks_impl(session_id):
            return self.__client.zero_blocks_async(
                self.__disk_id,
                start_index,
                blocks_count,
                session_id,
                idempotence_id,
                timestamp,
                trace_id,
                request_timeout)

        future = futures.Future()
        self._process_request(future, zero_blocks_impl, "ZeroBlocks")
        return future

    @_handle_errors
    def zero_blocks(
            self,
            start_index: int,
            blocks_count: int,
            idempotence_id: str | None = None,
            timestamp: datetime | None = None,
            trace_id: str | None = None,
            request_timeout: int | None = None):

        return self.zero_blocks_async(
            start_index,
            blocks_count,
            idempotence_id,
            timestamp,
            trace_id,
            request_timeout).result()

    @_handle_errors
    def _process_mount_response(self, volume: protos.TVolume, session_info: SessionInfo) -> None:
        if self.__volume is not None:
            # validate volume geometry
            if self.__volume.BlockSize != volume.BlockSize:
                raise ClientError(EResult.E_FAIL.value, "Volume geometry changed")

        self.log.debug("Complete MountVolume request")

        self.__volume = volume
        self.__session_info = session_info
        self.__state = _EMountState.MOUNT_COMPLETED

        self.__scheduler.cancel_all()

        if session_info.inactive_clients_timeout != 0:
            def remount():
                self._remount_volume()

            self.__scheduler.schedule(
                session_info.inactive_clients_timeout,
                remount)

    @_handle_errors
    def _remount_volume(self) -> futures.Future | None:
        unmounted = False
        idempotence_id = None
        trace_id = None
        request_timeout = None
        with self.__mount_lock:
            unmounted = (self.__state == _EMountState.UNINITIALIZED)
            idempotence_id = self.__mount_headers.IdempotenceId
            trace_id = self.__mount_headers.TraceId
            request_timeout = self.__mount_headers.RequestTimeout

        if unmounted:
            return None

        self.log.debug("Remount volume")

        future = self.__client.mount_volume_async(
            self.__disk_id,
            self.__mount_token,
            self.__access_mode,
            self.__mount_mode,
            self.__throttling_disabled,
            self.__mount_seq_number,
            idempotence_id=idempotence_id,
            trace_id=trace_id,
            request_timeout=request_timeout)

        def process_mount_response(f):
            with self.__mount_lock:
                try:
                    result = f.result()
                    self._process_mount_response(result["Volume"], result["SessionInfo"])
                except ClientError as e:
                    self.log.error("MountVolume request failed with error: %s", e)
                    self._reset_state()
                    self.__state = _EMountState.MOUNT_REQUESTED

        future.add_done_callback(process_mount_response)
        return future

    def _reset_state(self) -> None:
        self.__state = _EMountState.UNINITIALIZED
        self.__volume = None
        self.__session_info = None
        self.__scheduler.cancel_all()

    @_handle_errors
    def _process_request(self, result: futures.Future, call: Callable[[str], futures.Future], request_name: str) -> None:
        session_id_future = self._ensure_volume_mounted()

        def process_session_id_ready(f):
            session_id = ""
            try:
                session_id = f.result()
            except ClientError as e:
                result.set_exception(e)
                return

            self.log.debug("Submit %s request", request_name)
            call_future = call(session_id)

            def process_call_ready(f):
                try:
                    response = f.result()
                    self.log.debug("Complete %s request", request_name)
                    result.set_result(response)
                except ClientError as e:
                    if e.code != EResult.E_INVALID_SESSION.value:
                        self.log.debug("%s request failed: %s", request_name, e)
                        result.set_exception(e)
                        return

                    with self.__mount_lock:
                        self.__state = _EMountState.MOUNT_REQUESTED

                    self._process_request(result, call, request_name)
            call_future.add_done_callback(process_call_ready)

        session_id_future.add_done_callback(process_session_id_ready)

    @_handle_errors
    def _ensure_volume_mounted(self) -> futures.Future:
        session_id = ""
        needs_remount = False

        result = futures.Future()
        with self.__mount_lock:
            if self.__state == _EMountState.UNINITIALIZED:
                error = ClientError(
                    EResult.E_INVALID_STATE.value,
                    "Volume is not mounted")
                self.log.debug(error)
                result.set_exception(error)
                return result

            needs_remount = (self.__state == _EMountState.MOUNT_REQUESTED)
            if self.__session_info is not None:
                session_id = self.__session_info.session_id

        if not needs_remount:
            result.set_result(session_id)
            return result

        future = self._remount_volume()

        def process_mount_response(f):
            exception = f.exception()
            if exception is not None:
                result.set_exception(exception)
            else:
                r = f.result()
                result.set_result(r["SessionInfo"].session_id)
        future.add_done_callback(process_mount_response)

        return result
