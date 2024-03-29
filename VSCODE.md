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

### Git hooks

Enable git hooks for pre-commit checks
```
git config core.hooksPath .githooks
```
