# YDB stable-26-3-1 sync experiment

This branch contains an experimental import of `ydb-platform/ydb` branch `stable-26-3-1` into NBS.

The import keeps the current NBS-style layout:

- upstream `ydb/*` -> `contrib/ydb/*`
- upstream `yql/essentials/*` -> `contrib/ydb/library/yql/*`
- upstream `yql/providers/*` -> `contrib/ydb/library/yql/providers/*`
- upstream `yt/yql/providers/*` -> `contrib/ydb/library/yql/providers/*`

This mirrors the existing import patches better than exposing top-level `yql/essentials` and `yt/yql/providers` in NBS.

## Current Status

Verified on `dev` in `/home/apkobzev/work/nbs-ydb-folded-layout-test`:

```bash
./ya make -r cloud/storage/core/libs/actors
```

This passed before overlaying YDB 26.3 `contrib/libs/protobuf` and `contrib/libs/protoc` 22.5.

The current branch state is not green yet. Both the smoke target and the unit-test build currently fail early:

```bash
./ya make -r cloud/storage/core/libs/actors
./ya make -r -t cloud/storage/core/libs/actors/ut
```

Current blocker:

```text
Error: Unknown type in NPath::GetType: $name$.proto.h
YMake failed with exit code 1
```

This happens after the folded YQL/YT layout issues are gone. The next blocker is compatibility of the current NBS `ya`/ymake stack with YDB 26.3 `contrib/libs/protobuf` and `contrib/libs/protoc` 22.5, or finding a way to keep the older NBS protobuf/protoc while importing the newer YDB/YQL/YT layout.

## Nix Shell

The repository contains `flake.nix` with a small shell for local sync work.

```bash
nix develop
```

The shell provides common tools used by the sync script: `git`, `rsync`, GNU `sed`, `perl`, `python3`, and coreutils.

`flake.lock` is intentionally not committed yet because this experiment was prepared on a host without `nix`. Generate it locally when needed:

```bash
nix flake lock
```

## Running The Sync Locally

From a clean NBS checkout on the target branch:

```bash
nix develop

YDB_SRC=/path/to/ydb-stable-26-3-1 \
IMPORT_CONTRIB_DIR=/path/to/arcadia/kikimr/scripts/oss/import_contrib \
tools/sync-ydb-stable-26-3-1.sh
```

If `YDB_SRC` is not set, the script clones `https://github.com/ydb-platform/ydb.git` branch `stable-26-3-1` into `.sync/ydb-stable-26-3-1`.

The script expects a clean working tree unless `ALLOW_DIRTY=1` is set.

After sync, run:

```bash
./ya make -r cloud/storage/core/libs/actors
./ya make -r -t cloud/storage/core/libs/actors/ut
```

At the moment both commands document the same protobuf/protoc compatibility blocker.
