# Build Graph Helpers

Helpers in this directory inspect ya graphs to check target reachability and
prefetch build/test resources.

## Prefetching Resources For Runner Images

`prefetch_ya_resources.py` warms ya's native tool and resource caches without
compiling targets, running tests, or starting recipes. Run it as the eventual
runner user during image provisioning:

```bash
python3 .github/scripts/build_graph/prefetch_ya_resources.py \
    --cache-dir /home/github/.ya \
    --save-plan /tmp/ya-prefetch-plan.json \
    -- -A --build=relwithdebinfo -DGITHUB_CI=yes \
    cloud/blockstore cloud/filestore
```

Everything after `--` is passed to **graph generation only**. Use the same
targets, test filters, build flags and target platforms as CI. `-A` includes
tests of all sizes; omit it for a build-only graph. `--test-prepare` is not
required. For cross compilation, pass the usual `--target-platform=...`.

The script first runs `ya make -j0 --dump-json-graph`, which can download the
tools required to configure the graph. It follows dependencies from the graph's
results, collects referenced tools from `conf.resources` and per-node resource
URIs, and generates a separate graph containing only `/bin/true` commands.
Executing that graph with `--build-custom-json` lets ya perform its normal
fetching, extraction, locking and cache registration. Neither original node
commands nor their output UIDs are reused. The no-op nodes are uncacheable so
subsequent warmups check for resources that may have been evicted.

Both `build/mapping.conf.json` and `build/ext_mapping.conf.json` are resolved by
ya itself. The mapping files are catalogs; unused entries are not prefetched.
Tools are extracted into the tool cache. Other resources are stored in the
native build cache under ya's resource UIDs; a later build may still unpack or
copy them, but does not need to download them again.

To inspect a plan first, or reuse saved graphs:

```bash
python3 .github/scripts/build_graph/prefetch_ya_resources.py \
    --plan-only --save-graph /tmp/full-graph.json \
    --save-plan /tmp/prefetch-plan.json \
    -- -A --build=relwithdebinfo cloud/storage

python3 .github/scripts/build_graph/prefetch_ya_resources.py \
    --cache-dir /home/github/.ya \
    --graph-json /tmp/full-graph.json
```

Repeat `--graph-json` to warm the union of several builds. Tool definitions with
the same pattern name but different resources remain separate. Use graphs from
the same checkout and ya version, for the intended host OS/architecture. Ya
selects the host variant of platform-dependent tool definitions.

`--jobs` controls prefetch concurrency (default: 8). `--ya-bin` selects a launcher
or binary. Repeat `--ya-arg=...` for options needed in both phases, such as
`--ya-arg=--cache-size=100G` or `--ya-arg=--host-platform=...`. Existing environment
overrides, including `YA_CACHE_DIR_TOOLS`, are honored. Downloads fail the command
with a nonzero exit code; a partial cache can be reused on retry.

For an image, retain both `.ya/tools` and the persistent cache under `.ya/build`.
Use the same cache settings/user at runtime, keep `clean_ya_dir=no`, and size the
image disk and cache limits to fit the selected resources. `--keep-temps` is not
needed. Source changes that introduce new resource IDs require another warmup.
Downloads implemented directly inside build commands, tests or recipes are
outside this script's scope. It does not run those commands or populate caches
of compiled outputs. The generated graph format is an internal ya interface.

Validation:

```bash
python3 -m pytest -q .github/scripts/build_graph
```

## Cloud Reachability Check

Run from the repository root:

```bash
python3 .github/scripts/build_graph/check_reachable_yamakes.py \
    --ignore-file .github/scripts/build_graph/cloud-reachability-ignore.txt \
    --print-reachable-count
```

By default this checks all `cloud/**/ya.make` files from these roots:

```text
cloud/blockstore
cloud/disk_manager
cloud/fastshard
cloud/filestore
cloud/storage
cloud/tasks
```

The script sets `YA_CACHE_DIR=/tmp/ya-cache` if it is not already set and runs:

```bash
./ya dump dir-graph --noya-tc -t --split <roots...>
```

`-t` is required because it enables `RECURSE_FOR_TESTS` traversal.

## Reusing A Saved Graph

When iterating, save the graph once and reuse it:

```bash
python3 .github/scripts/build_graph/check_reachable_yamakes.py \
    --save-graph-json /tmp/cloud-dir-graph.json \
    --ignore-file .github/scripts/build_graph/cloud-reachability-ignore.txt

python3 .github/scripts/build_graph/check_reachable_yamakes.py \
    --graph-json /tmp/cloud-dir-graph.json \
    --ignore-file .github/scripts/build_graph/cloud-reachability-ignore.txt
```

## Modes

Default mode is `ya-style`:

```text
RECURSE closure from roots, then INCLUDE deps from those recurse-reachable dirs.
```

This matches the useful practical check: it excludes helper libraries that are
reachable as ordinary build dependencies.

For a stricter view, use:

```bash
python3 .github/scripts/build_graph/check_reachable_yamakes.py --mode recurse
```

That follows only `RECURSE` edges and reports more helper targets.

## Interpreting Results

Output paths are `ya.make` files that are not reachable under the selected mode.
A real test target in this list usually means its parent needs a
`RECURSE_FOR_TESTS(...)` entry, or an ancestor library directory needs a
`RECURSE(...)` entry so its own test recurse can be traversed.

Known intentional exceptions belong in `cloud-reachability-ignore.txt` with a
short explanation.
