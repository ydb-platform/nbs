"""Read-only, bounded access to a backup bucket. Never print signed URLs."""

import json
import math
import os
import re
import selectors
import signal
import stat
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Protocol
from urllib.parse import unquote, urlsplit


class BackupError(Exception):
    """A safe-to-log backup validation failure."""


class InvalidBackup(BackupError):
    """Published backup is malformed, incomplete or fails integrity checks."""


class PendingBackup(BackupError):
    """Snapshot metadata or the final chunk map has not appeared yet."""


class AuthError(BackupError):
    """Reader credentials or a required decryption key are unavailable."""


class TransportError(BackupError):
    """Backup storage or presigning service is unavailable."""


class BackupTimeout(BackupError):
    """The caller's monotonic deadline was reached."""


class ObjectNotFound(BackupError):
    """The requested backup object does not exist."""


def require_read_only_trace(paths):
    """Fail closed unless all configured provider trace directories are read-only.

    Suppressing stderr does not disable a provider's on-disk API trace.
    Deployment must enumerate every trace path used by its provider adapter.
    Permissions alone are insufficient (the tester runs as root). The hardened
    systemd unit must provide a genuinely read-only filesystem/mount instead.
    """
    message = ("Provider file trace is not protected by a read-only filesystem; "
               "run through the hardened snapshot-backup-tester systemd unit")
    try:
        if not isinstance(paths, (list, tuple)) or not paths:
            raise ValueError("Missing trace paths")
        for value in paths:
            if not isinstance(value, str):
                raise ValueError("Invalid trace path")
            path = Path(value)
            if (not path.is_absolute() or ".." in path.parts
                    or any(ord(char) < 32 or ord(char) == 127 for char in str(path))):
                raise ValueError("Unsafe trace path")
            nearest = None
            # Also inspect ancestors: a symlink can redirect an otherwise safe
            # default to a writable location, or be replaced after this check.
            for candidate in (path, *path.parents):
                try:
                    info = candidate.lstat()
                except (FileNotFoundError, PermissionError):
                    # ProtectHome may hide children completely. Confirm the
                    # mount at its closest inspectable ancestor instead.
                    continue
                if stat.S_ISLNK(info.st_mode):
                    raise ValueError("Redirected trace path")
                if nearest is None:
                    if not stat.S_ISDIR(info.st_mode):
                        raise ValueError("Invalid trace directory")
                    nearest = candidate
            if nearest is None or not os.statvfs(nearest).f_flag & os.ST_RDONLY:
                raise ValueError("Writable or unknown trace filesystem")
    except (OSError, ValueError, RuntimeError, AttributeError):
        raise AuthError(message) from None


def remaining(deadline):
    value = deadline - time.monotonic()
    if not math.isfinite(value) or value <= 0:
        raise BackupTimeout("Backup read deadline exceeded")
    return value


@dataclass(frozen=True, repr=False)
class StoredObject:
    data: bytes
    metadata: Mapping[str, str]

    def __post_init__(self):
        normalized = {}
        for key, value in self.metadata.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise InvalidBackup("Invalid object metadata")
            key = key.lower().removeprefix("x-amz-meta-")
            if key in normalized:
                raise InvalidBackup("Duplicate object metadata")
            normalized[key] = value
        object.__setattr__(self, "metadata", MappingProxyType(normalized))
        object.__setattr__(self, "data", bytes(self.data))


class ObjectStore(Protocol):
    def get(self, key: str, *, deadline: float, max_bytes: int) -> StoredObject:
        """Get a relative backup key, or raise a sanitized BackupError."""


def _validate_key(key):
    if (not isinstance(key, str) or not key or len(key.encode()) > 1024
            or any(ord(char) < 32 or ord(char) == 127 for char in key)
            or any(part in ("", ".", "..") for part in key.split("/"))
            or "\\" in key):
        raise InvalidBackup("Invalid backup object key")


def interrupt_action(_signum, _frame):
    """Unwind the current call so its process-group cleanup runs on cancellation."""
    raise SystemExit(2)


def run_command(command, *, deadline, timeout, input_data=None):
    """Bound both wall time and stdout; no command/output in exceptions."""
    remaining(deadline)
    call_deadline = min(deadline, time.monotonic() + timeout)
    process = None
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE if input_data else subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                   start_new_session=True)
        result = bytearray()
        offset = 0
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            if input_data:
                os.set_blocking(process.stdin.fileno(), False)
                selector.register(process.stdin, selectors.EVENT_WRITE)
            while selector.get_map():
                for event, _ in selector.select(remaining(call_deadline)):
                    if event.fileobj is process.stdout:
                        chunk = os.read(process.stdout.fileno(), 65536)
                        if not chunk:
                            selector.unregister(process.stdout)
                        else:
                            result.extend(chunk)
                            if len(result) > 1024 * 1024:
                                raise TransportError("Oversized backup command response")
                    else:
                        offset += os.write(process.stdin.fileno(), input_data[offset:offset + 65536])
                        if offset == len(input_data):
                            selector.unregister(process.stdin)
                            process.stdin.close()
        code = process.wait(timeout=remaining(call_deadline))
        remaining(deadline)
        return subprocess.CompletedProcess(command, code, bytes(result))
    except subprocess.TimeoutExpired:
        raise BackupTimeout("Backup command timed out") from None
    except OSError:
        raise TransportError("Backup command could not be executed") from None
    finally:
        if process is not None:
            # Terminate descendants even when the adapter itself has exited.
            # Every invocation owns a new process group, never the tester group.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            process.stdout.close()
            if process.stdin is not None:
                process.stdin.close()


class PresignedObjectStore:
    """Deployment adapter -> single GET presign -> HTTPS download.

    Only backup-bucket reads are implemented: no list, write, delete or source
    fallback. The presign host is explicit, and the resulting URL must identify
    that host and the exact requested object. curl receives the URL on stdin,
    never argv. Its own config, redirects, proxies and retries are disabled.
    """

    def __init__(self, profile, bucket, presign_host, prefix="", *,
                 executable, config_path, trace_paths, curl_executable="curl",
                 request_timeout=30):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", presign_host or ""):
            raise ValueError("A plain HTTPS presign hostname is required")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", bucket or ""):
            raise ValueError("A backup bucket is required")
        if not profile or not math.isfinite(request_timeout) or request_timeout <= 0:
            raise ValueError("A profile and a positive request timeout are required")
        if prefix:
            _validate_key(prefix)
        self.profile = profile
        self.bucket = bucket
        self.host = presign_host.lower()
        self.prefix = prefix
        self.executable = executable
        self.config_path = config_path
        self.trace_paths = trace_paths
        self.curl_executable = curl_executable
        self.request_timeout = request_timeout
        self._curl_checked = False

    def _check_curl(self, deadline):
        if self._curl_checked:
            return
        result = self._run([self.curl_executable, "-q", "--version"],
                           deadline=deadline, timeout=self.request_timeout)
        version = re.match(rb"curl ([0-9]+)\.([0-9]+)\.([0-9]+)\b", result.stdout)
        if result.returncode or not version or tuple(map(int, version.groups())) < (8, 4, 0):
            raise TransportError("curl >= 8.4 is required for bounded downloads")
        self._curl_checked = True

    _run = staticmethod(run_command)

    def _presign(self, key, directory, deadline):
        require_read_only_trace(self.trace_paths)
        command = [self.executable, "--config", str(self.config_path)]
        envelope = {"version": 1, "operation": "presign", "profile": self.profile,
                    "request": {"bucket": self.bucket, "host": self.host,
                                "key": key, "method": "GET", "expires_seconds": 900}}
        result = self._run(command, deadline=deadline, timeout=self.request_timeout,
                           input_data=json.dumps(envelope).encode())
        if result.returncode:
            # A provider failure may mean either denied access or an outage.
            raise TransportError("Backup presign failed; check provider profile and access")
        if len(result.stdout) > 1024 * 1024:
            raise TransportError("Oversized presign response")
        try:
            document = json.loads(result.stdout)
        except (ValueError, UnicodeError):
            raise TransportError("Invalid presign response") from None
        candidates = set()
        pending = [document]
        while pending:
            value = pending.pop()
            if isinstance(value, dict):
                pending.extend(value.values())
            elif isinstance(value, list):
                pending.extend(value)
            elif isinstance(value, str) and value.startswith("https://"):
                try:
                    parsed = urlsplit(value)
                    safe = (parsed.scheme == "https" and parsed.hostname == self.host
                            and parsed.port in (None, 443) and parsed.username is None
                            and parsed.password is None and not parsed.fragment
                            and not any(ord(c) < 32 or ord(c) == 127 for c in value)
                            and unquote(parsed.path).lstrip("/") in (key, self.bucket + "/" + key))
                except ValueError:
                    safe = False
                if safe:
                    candidates.add(value)
        if len(candidates) != 1:
            raise TransportError("Presign response does not identify one expected object")
        return candidates.pop()

    @staticmethod
    def _headers(raw):
        if len(raw) > 64 * 1024:
            raise TransportError("Oversized storage response headers")
        metadata = {}
        for line in raw.decode("iso-8859-1").splitlines():
            if line.startswith("HTTP/"):
                metadata = {}  # Discard CONNECT / informational response headers.
            elif line.lower().startswith("x-amz-meta-"):
                if ":" not in line:
                    raise InvalidBackup("Malformed storage metadata")
                key, value = line.split(":", 1)
                key = key.lower()[11:]
                if key in metadata:
                    raise InvalidBackup("Duplicate storage metadata")
                metadata[key] = value.strip()
        return metadata

    def get(self, key, *, deadline, max_bytes):
        _validate_key(key)
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes <= 0:
            raise ValueError("Positive object size limit required")
        full_key = self.prefix + "/" + key if self.prefix else key
        _validate_key(full_key)
        remaining(deadline)
        self._check_curl(deadline)
        try:
            with tempfile.TemporaryDirectory(prefix="snapshot-backup-get-") as directory:
                url = self._presign(full_key, directory, deadline)
                data_path = Path(directory) / "body"
                headers_path = Path(directory) / "headers"
                # -q must be the first option: do not load ~/.curlrc (debugging,
                # redirects or proxy settings there could leak the signed URL).
                command = [self.curl_executable, "-q", "--silent", "--fail", "--globoff",
                           "--proto", "=https", "--noproxy", "*", "--retry", "0",
                           "--connect-timeout", str(self.request_timeout),
                           "--max-time", str(min(self.request_timeout, remaining(deadline))),
                           "--max-filesize", str(max_bytes), "--output", str(data_path),
                           "--dump-header", str(headers_path), "--write-out", "%{http_code}",
                           "--config", "-"]
                quoted_url = url.replace("\\", "\\\\").replace('"', '\\"')
                result = self._run(command, deadline=deadline, timeout=self.request_timeout,
                                   input_data=('url = "' + quoted_url + '"\n').encode())
                status = result.stdout.strip()
                if status == b"404":
                    raise ObjectNotFound("Backup object does not exist")
                if status in (b"401", b"403"):
                    raise AuthError("Backup storage denied access")
                if result.returncode == 28:
                    raise BackupTimeout("Backup object download timed out")
                if result.returncode == 63:
                    raise InvalidBackup("Backup object exceeds size limit")
                if result.returncode or status != b"200":
                    raise TransportError("Backup object download failed")
                with data_path.open("rb") as stream:
                    body = stream.read(max_bytes + 1)
                if len(body) > max_bytes:
                    raise InvalidBackup("Backup object exceeds size limit")
                with headers_path.open("rb") as stream:
                    metadata = self._headers(stream.read(64 * 1024 + 1))
                remaining(deadline)
                return StoredObject(body, metadata)
        except OSError:
            raise TransportError("Unable to use private download directory") from None
