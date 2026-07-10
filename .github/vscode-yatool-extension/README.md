# Yatool ya.make helpers

Small VS Code extension for navigating common path-bearing `ya.make` macros.

## Features

- Document links for `INCLUDE(...)`.
- Document links for local `DATA(...)` arguments such as `arcadia/cloud/...`.
- Document links for `RECURSE(...)`, `RECURSE_FOR_TESTS(...)`, `RECURSE_ROOT_RELATIVE(...)`, and `PEERDIR(...)`.
- Go to definition and hover information for `FROM_SANDBOX(...)` numeric resource ids found in `build/ext_mapping.conf.json` or `build/mapping.conf.json`.
- Hover hints for common macros such as `PY3TEST`, `SPLIT_FACTOR`, `PEERDIR`, `RECURSE`, `RECURSE_FOR_TESTS`, `INCLUDE`, `DATA`, `TEST_SRCS`, `DEPENDS`, and `SET`.
- Diagnostics for missing resolved paths.
- Diagnostics when `INCLUDE(...)` resolves to a directory.
- Diagnostics when module-directory macros do not resolve to a directory with `ya.make`.
- Basic syntax diagnostics for unclosed macro calls.
- Basic syntax highlighting for `ya.make` and `.inc` files.

Supported path forms:

- `INCLUDE(${ARCADIA_ROOT}/path/to/file.inc)`
- `INCLUDE(${CURDIR}/file.inc)`
- `INCLUDE(relative/file.inc)`
- `DATA(arcadia/path/to/data)`
- `DATA(${ARCADIA_ROOT}/path/to/data)`
- `RECURSE(relative/module)`
- `RECURSE_FOR_TESTS(relative/test/module)`
- `RECURSE_ROOT_RELATIVE(root/relative/module)`
- `PEERDIR(root/relative/module)`
- `FROM_SANDBOX(FILE 12500613127 RENAME RESOURCE OUT_NOAUTO rootfs.img)`

Remote or non-filesystem data forms such as `sbr://...` and `ext:...` are ignored by this first version.

## Local Use

### Option 1: Run From Source

Use this while developing the extension:

1. Open `.github/vscode-yatool-extension`.
2. Press `F5` or choose `Run and Debug: Launch Extension`.
3. A second VS Code window opens with `[Extension Development Host]` in the title.
4. In that second window, open the NBS repository root.
5. Open a `ya.make` file and use `Ctrl+Click` / `Cmd+Click` on supported paths.

If you currently have the NBS repo root open, use `File: Open Folder...` from the command palette and select `/home/librarian/nbs/.github/vscode-yatool-extension` first. Pressing `F5` from the repo root will not use this extension's launch config.

### Option 2: Install Locally By Symlink

Use this if you want the extension available in your normal VS Code window:

```bash
mkdir -p ~/.vscode/extensions
ln -sfnT /home/librarian/nbs/.github/vscode-yatool-extension ~/.vscode/extensions/local.vscode-yatool-extension-0.0.1
```

Then reload VS Code.

If VS Code is already open, run `Developer: Reload Window` from the command palette.

If you are connected through VS Code Remote SSH / VS Code Server, install it on the remote side instead:

```bash
mkdir -p ~/.vscode-server/extensions
ln -sfnT /home/librarian/nbs/.github/vscode-yatool-extension ~/.vscode-server/extensions/local.vscode-yatool-extension-0.0.1
```

Then run `Developer: Reload Window` in the remote VS Code window.

### Option 3: Install From Workflow Artifact

The `Build VS Code yatool extension` GitHub Actions workflow packages this extension as a `.vsix` file.

1. Open the workflow run in GitHub Actions.
2. Download the `vscode-yatool-extension` artifact.
3. Extract the artifact; it contains `vscode-yatool-extension.vsix`.
4. In VS Code, run `Extensions: Install from VSIX...` from the command palette.
5. Select the `.vsix` file and reload VS Code when prompted.

## Check That It Loaded

After launching or installing:

1. Open the command palette.
2. Run `Developer: Show Running Extensions`.
3. Look for `Yatool ya.make helpers`.

You can also open the command palette and search for `Yatool: Refresh ya.make Diagnostics`. If it does not appear, VS Code has not loaded the extension.

## Notes

This extension intentionally uses a small tolerant scanner instead of trying to fully evaluate ymake. It is meant for fast editor feedback; authoritative build-system validation should still come from `ya`/`ymake`.

Full ymake syntax and semantic validation is significantly larger than this extension's scanner. Yatool has the real parser in `devtools/ymake/lang`, and it understands macro signatures, conditions, variables, plugins, and module state. This extension currently checks only editor-friendly cases that can be detected from one file plus local mapping JSON files.
