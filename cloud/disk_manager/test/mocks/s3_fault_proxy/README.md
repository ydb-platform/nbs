# Local S3 fault proxy

The proxy supports isolated object-store recipes only. It rejects external upstream endpoints and requires no cloud credentials. Never deploy it in front of a shared object store. Route only backup writes through the proxy; primary storage operations and validation reads should use the local emulator directly.

## Build and run

```bash
./ya make cloud/disk_manager/test/mocks/s3_fault_proxy/cmd/s3_fault_proxy

cloud/disk_manager/test/mocks/s3_fault_proxy/cmd/s3_fault_proxy/s3_fault_proxy \
  --upstream http://127.0.0.1:5000 \
  --listen 127.0.0.1:5001 \
  --control-listen 127.0.0.1:5002
```

These ports are examples; recipes allocate available ports. Both listeners require explicit `127.0.0.1`. Termination releases held requests and bounds handler shutdown to five seconds.

## Control API

Enable a 60-second failure for one backup-write prefix:

```bash
curl --fail --max-time 5 \
  -H 'Content-Type: application/json' \
  --data '{"mode":"fail503","method":"PUT","path_prefix":"/snapshot-backup/recipe/","ttl_seconds":60}' \
  http://127.0.0.1:5002/fault
curl --fail --max-time 5 http://127.0.0.1:5002/status
curl --fail --max-time 5 -X POST http://127.0.0.1:5002/reset
```

`DELETE /fault` also resets the fault. Initial and reset states are `mode: "pass"`, `active: false`.

`POST /fault` requires method PUT, a nonempty path prefix other than `/`, and a TTL from 1 to 300 seconds. Unknown fields, modes, and invalid TTLs return HTTP 400. Replacing a fault, resetting it, or expiry releases requests held by that fault.

| Mode | Behavior |
| --- | --- |
| `fail503` | Return HTTP 503 / S3 ServiceUnavailable without forwarding. |
| `drop-before-write` | Close the connection before forwarding the write. |
| `drop-after-success` | Forward the write, then close the connection without returning its successful response. |
| `hold-before-write` | Hold before forwarding until reset or expiry. |
| `hold-after-success` | Forward the write, then hold its successful response until reset or expiry. |

Status reports active mode and expiry, fault hits, upstream-accepted writes, and currently held successful responses. A fault scenario needs a positive hit count. `held_after_success` is an active gauge; it is distinct from the cumulative accepted-write count.

Reset preserves the last fault's cumulative counters; a new fault clears them. Object names, request bodies, store responses, and keys are not logged. This proxy alone does not test scheduler retry exhaustion; that requires actual task-runner retries.

## Verification

```bash
./ya make -tt cloud/disk_manager/test/mocks/s3_fault_proxy/tests
```

The unit tests cover method/prefix selection, pre-write errors, lost successful responses, cancellation, S3 SlowDown, TTL/reset validation, and release on expiry or controller shutdown. Passing proxy tests alone does not prove the integration recipes pass.
