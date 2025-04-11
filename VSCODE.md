## Visual Studio Code

### Generate workspace

Run vscode_generate_workspace.sh to generate a workspace in the root of the repository.
```
.vscode_generate_workspace.sh
```

### Open workspace

Open workspace from menu "Open workspace from file..." And select the newly created file "nbs.code-workspace" in the root of the repository.

Or execute
```
code nbs.code-workspace
```

### Code competition

Install all recommended by workspace plugins.
The most important one is https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd used for code competition and formatting.

### Formatting

Enable feature "Trim trailing whitespace" for user or workspace.
Enable feature "Trim final newlines" for user or workspace.
Enable feature "Insert final newline" for user or workspace.
Enable Go-specific 80 column vertical ruler for user or workspace.

You can add all of the above by adding the following code to your `settings.json`:
```
    "files.trimTrailingWhitespace": true,
    "files.trimFinalNewlines": true,
    "files.insertFinalNewline": true,
    "[go]": {
        "editor.rulers": [
            {
                "column": 80,
                "color": "#ff0a0a"
            }
        ],
    },
```

### Git hooks

Enable git hooks for pre-commit checks
```
git config core.hooksPath .githooks
```

### Debugging

If you want to use debugging in VS Code you should to enable the static linkage.

Add section below to '~/.ya/ya.conf'
```
[[target_platform]]
platform_name = "default-linux-x86_64"
build_type = "relwithdebinfo"
#build_type = "release"

[target_platform.flags]
FORCE_STATIC_LINKING="yes"
```
