# Build static qemu emulator

\# Build from sources
```
build-qemu --src 'src-dir' [--deps]
```
Default tarball name is `qemu-static-<version>-<arch>.tgz`. Pass `--out`
to use an exact path.

\# Build from github
```
build-qemu --co --git-tag v6.0.0-rc4 [--deps]
```

# Update qemu used in tests

\# Upload qemu to sandbox
```
ya upload --ttl inf -a linux -d 'qemu static build' qemu-static-<version>-<arch>.tgz
```

Update resource id and list of files from `tar --list -f qemu-static-<version>-<arch>.tgz` in
```
./tools/testing/qemu/bin/ya.make
```

# Samples

```
./__main__.py --co --git-tag v11.0.2 --git https://github.com/qemu/qemu --deps
./__main__.py --co --git-tag v10.2.4 --git https://github.com/qemu/qemu --deps
./__main__.py --co --git-tag v9.2.3 --git https://github.com/qemu/qemu --deps
./__main__.py --co --git-tag v8.2.10 --git https://github.com/qemu/qemu --deps
./__main__.py --co --git-tag v7.2.22 --git https://github.com/qemu/qemu --deps
./__main__.py --co --git-tag v7.1.0  --git https://github.com/qemu/qemu --deps
./__main__.py --co --git-tag v6.2.0  --git https://github.com/qemu/qemu --deps
./__main__.py --co --git-tag v5.2.0  --git https://github.com/qemu/qemu --deps

```
