# Build static qemu emulator

\# Build static libslirp for qemu 7.0+ `-netdev user`
```
./build-libslirp-static.sh --ref v4.9.3
export PKG_CONFIG_PATH="$PWD/libslirp-static/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
```
The qemu build script also runs this helper automatically when static libslirp
is missing for qemu 7.0+ builds.

\# Build from sources
```
build-qemu --src 'src-dir' [--deps]
```

\# Build from github
```
build-qemu --co --git-tag v6.0.0-rc4 [--deps]
```

# Update qemu used in tests

\# Upload qemu to sandbox
```
ya upload --ttl inf -a linux -d 'qemu static build' qemu-static.tgz
```

Update resource id and list of files from 'tar --list -f qemu-static.tgz' in
```
./tools/testing/qemu/bin/ya.make
```
