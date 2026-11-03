# Deployment adapter protocol

The public tester contains no cloud-provider implementation. Deploy an executable adapter separately and set its absolute path in `provider_command`. Keep its credentials, endpoints, profile definitions, provisioning instructions, and implementation-specific diagnostics in private deployment configuration.

The tester invokes:

```text
/absolute/path/to/adapter --config /absolute/path/to/private-config
```

Each invocation receives one UTF-8 JSON object on stdin and returns one JSON object on stdout, limited to 1 MiB while reading. Exit zero means the synchronous operation succeeded. Any other exit blocks the call; stderr is suppressed. Empty stdout is not a successful JSON response. Do not put tokens, signed URLs, requests, or responses in command arguments or logs.

No interactive authentication or automatic mutation retry is allowed. A timed-out create has an unknown outcome and must be reconciled using its journalled name and ownership labels. The adapter must wait for terminal operation success, not return an asynchronous operation ID as a snapshot ID.

The version field is `1`. Unknown versions, operations, methods, or unsupported fields must fail closed. Profile names are opaque deployment-local identifiers.

## Compute requests

```json
{
  "version": 1,
  "operation": "compute",
  "profile": "compute-reader-and-snapshot-writer",
  "resource": "disk",
  "method": "get",
  "request": {"disk_id": "synthetic-disk"}
}
```

Only these operations are needed:

| Resource / method | Request | Required normalized response |
| --- | --- | --- |
| instance / get | instance_id | id, folder_id, zone_id, status, labels, boot_disk, secondary_disks |
| disk / get | disk_id | id, folder_id, zone_id, size |
| snapshot / create | folder_id, disk_id, name, labels | id, source_disk_id, folder_id, name, labels, status |
| snapshot / get | snapshot_id | id, source_disk_id, folder_id, name, labels |
| snapshot / delete | snapshot_id | Empty object after confirmed completion |

`folder_id` names the configured resource-isolation scope; an adapter can map a provider's project or tenant to this field. `zone_id` must identify the actual deployment zone. Do not fabricate identity or ownership fields from the request when the provider did not return them.

An instance response must report status `RUNNING`, label `purpose=snapshot-backup-e2e`, `boot_disk: {"disk_id": "..."}`, and `secondary_disks` entries containing `disk_id`, `device_name`, and `mode`. Data disks must be attached `READ_WRITE`. A disk size is its exact byte count, encoded as an integer or decimal string.

Snapshot creation must return a `READY` resource with the original name, source disk, scope, and labels preserved. A `{"response": <snapshot>}` wrapper is accepted; an operation ID alone is not.

Before destructive I/O, the tester independently reads its local instance ID from the standard metadata endpoint `http://169.254.169.254/latest/meta-data/instance-id`, without proxy use. The deployment must support this identity endpoint. A provider adapter alone does not make other metadata formats supported.

## Read-only object presigning

```json
{
  "version": 1,
  "operation": "presign",
  "profile": "backup-reader",
  "request": {
    "bucket": "backup-bucket",
    "host": "objects.example.test",
    "key": "prefix/chunks/example",
    "method": "GET",
    "expires_seconds": 900
  }
}
```

Return `{"url": "https://objects.example.test/..."}`. The URL must reference exactly the requested host and object, with HTTPS and the default TLS port, without embedded credentials or redirects. Path-style bucket URLs and direct object paths are accepted. The tester does not list, write, or delete bucket objects.

The signed URL is passed to curl over stdin, never argv. curl configuration, proxies, redirects, and retries are disabled; downloads have time and size bounds.

## Adapter security obligations

- Use noninteractive, narrowly scoped credentials that renew without modifying shared services.
- Never log credentials, keys, signed URLs, API bodies, or private configuration.
- Discover all trace paths used by the adapter and any child tools. Configure every path in `provider_trace_paths` and keep their filesystems read-only inside the hardened unit. The tester checks mount flags and rejects symlinked paths; chmod alone is insufficient for a root process.
- Do not create an alternate writable trace location. A fake or incomplete trace inventory does not satisfy the contract.
- Keep credential renewal caches separate from API traces and allow only the minimal private cache path.
- Do not detach or create new sessions for child commands. Each invocation runs in its own process group, which the tester terminates on success, failure, or timeout. The systemd unit provides an additional whole-service boundary.
- Validate response identity against provider data; never turn arbitrary errors or missing resources into successful deletion.
- Independently smoke-test the adapter in the target sandbox before enabling continuous execution. Public unit tests use fakes and do not certify a private implementation.
