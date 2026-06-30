# Build Graph Helpers

Helpers in this directory inspect `ya dump dir-graph --split` output. They are
intended for catching `ya.make` files that exist in the checkout but are not
reachable from the expected `RECURSE` / `RECURSE_FOR_TESTS` graph.

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
