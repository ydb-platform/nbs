# YDB stable-26-3-1 sync experiment

This branch contains an experimental import of `ydb-platform/ydb` branch `stable-26-3-1` into NBS.

The import keeps the current NBS-style layout:

- upstream `ydb/*` -> `contrib/ydb/*`
- upstream `yql/essentials/*` -> `contrib/ydb/library/yql/*`
- upstream `yql/providers/*` -> `contrib/ydb/library/yql/providers/*`
- upstream `yt/yql/providers/*` -> `contrib/ydb/library/yql/providers/*`

This mirrors the existing import patches better than exposing top-level `yql/essentials` and `yt/yql/providers` in NBS.

The sync also prunes YDB subtrees that are not needed by NBS or are absent from the current NBS `main` 24.4-style import. Documentation trees such as `contrib/ydb/docs` and `contrib/ydb/yql_docs` are removed entirely, together with the small docs-only build/test consumers that would otherwise point at those removed files.

The top-level sync script is only an orchestrator. Individual steps live under `tools/ydb-sync/`: folded-layout copy, import patch application, path rewrites, subtree pruning, and extra dependency copy are split into separate scripts.

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

This happens after the folded YQL/YT layout issues are gone. The same blocker is reached by the Nix sandbox check:

```bash
nix build .#checks.x86_64-linux.ya-make-smoke --print-build-logs
```

The Nix check successfully materializes the synced source tree, prefetches the `ya` bootstrap, `ymake`, and `ya-tc` tool resources, patches the external Linux ELF binaries for the Nix sandbox, and then fails at the same YMake protobuf/protoc compatibility point.

The next blocker is compatibility of the current NBS `ya`/ymake stack with YDB 26.3 `contrib/libs/protobuf` and `contrib/libs/protoc` 22.5, or finding a way to keep the older NBS protobuf/protoc while importing the newer YDB/YQL/YT layout.

## Nix Shell

The repository contains `flake.nix` with a small shell for local sync work and Nix targets for materializing the synced source tree.

```bash
nix develop
```

The shell provides common tools used by the sync script: `git`, `rsync`, GNU `sed`, `perl`, `python3`, and coreutils.

YDB is pinned as a non-flake input in `flake.nix`:

```nix
ydb-src.url = "github:ydb-platform/ydb/ffa7b99b42391d01548c55bb7117d61a0e74fc63";
```

Build the synthetic NBS + YDB source tree with:

```bash
nix build .#nbs-ydb-synced-src
```

This derivation runs the sync pipeline with the pinned YDB source and writes the resulting source tree into the Nix store. The sync order is also encoded in `flake.nix` as `syncSteps` and exposed as:

```bash
nix build .#ydb-sync-order
```

Run local convenience wrappers from the current checkout:

```bash
nix run .#ya-make-smoke
nix run .#ya-make-actors-ut
```

Run sandbox-style validation checks:

```bash
nix flake check
```

`flake.lock` is committed intentionally. It pins both nixpkgs and the exact YDB GitHub source revision used by the source derivation.

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
