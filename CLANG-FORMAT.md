## clang-format

Use [this](.clang-format) clang-format config to format new and modified code.

## Install

For VSCode install and use clangd plugin https://marketplace.visualstudio.com/items?itemName=llvm-vs-code-extensions.vscode-clangd

For other development environments, you should install clang-format-18 manually.

```
sudo apt-get install clang-format-18
```

Configure the plugin for your favorite code editor to use the [config](.clang-format) to format the code.

Note. clang-format searches for the .clang-format configuration file in the directory of the formatted file and in all parent directories. Therefore, no additional settings are required.
