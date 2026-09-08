# A tool to manipulate state files of write-back cache

### Description

A tool that may help investigate and resolve problems with write-back cache.


### Usage

Build `write_back_cache_state_tool` binary


### How to run

Commands:

`list` — list states and give summary in JSON format;

`check` — check integrity of the state file;

`dump` — output the contents of the state file in JSON format, the payload is
replaced with a hash;

`patch` — apply changes to the data returned by `dump` command.

Options:

`--state-dir (directory)` — path to a directory with write-back cache state
  files, the default value is `/Berkanavt/nfs-vhost/state`;

`--fs-id (filesystem)` — filesystem ID;

`--session-id (session)` — session ID (can be omitted if there is only a single
  session);

`--state-file (path_to_state_file)` — can be used instead of setting
  `--state-dir`, `--fs-id`, `--session-id`;

`-I`, `--input` — take input data from a file instead of stdin;

`-O`, `--output` — write command output to a file instead of stdout;

`--unsafe-ignore-lock` — do not check and acquire advisory lock;

`--unsafe-ignore-corruption` — allow patching corrupted files.


### How to patch

1. Generate state file dump using `dump` command.

2. Edit fields in the dump file.

3. Apply changed using `patch` command.

4. Verify state file using `check` command.

Patch will be rejected if the state file has changed since the dump was
generated. It is needed to repeat steps 1 and 2 in this case.

Fields that are allowed to change:

* Header: `ReadPos`, `WritePos`, `MetadataChecksum` (but not the contents);
* Entry header: `DataChecksum`, `Tag`, `FreeFlag`;
* Entry payload: `NodeId`, `Handle`, `Offset`.

It is not possible to change entry size, modify request data, add new entries.

Entry header and entry payload can be changed only when DataChecksum matches
the actual checksum.

Example scenarios:

1. Wipe all entries: set `ReadPos` and `WritePos` to zero.

2. Delete unwanted entries: set `FreeFlag` to `true`.

3. Fix checksum error: set `DataChecksum` to `ActualDataChecksum`.

4. Fix `E_FS_BADHANDLE` error: change `Handle` field to a live handle or change
`Tag` value from 0 to 1.


### Warning

Write-back cache state file stores unflushed client's requests. It may contain
sensitive data that should never be exposed. Therefore, the tool doesn't allow
to manipulate with requests except making minor changes to metadata.
