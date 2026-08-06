Source downloaded from https://gitlab.com/virtio-fs/virtiofsd/-/releases/v1.14.0
and built with:

```bash
./build.sh
```

The default tag is `v1.14.0`. The build is static-only and uses the current
Debian architecture from `dpkg --print-architecture`. The output is written to
`./virtiofsd_${VERSION}_${ARCH}`.

```bash
ARCH=amd64 TAG=v1.14.0 ./build.sh
```

Install package build dependencies before building:

```bash
./build.sh --install-build-deps
```
