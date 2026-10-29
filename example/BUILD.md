# Building the local NBS example

Build the package from the repository root:

```bash
./ya make cloud/blockstore/buildall -r
```

The package includes everything used by the scripts: the matching YDB server
and client, NBS server and client, disk agent, NBD adapter, vhost server, QEMU
archive, and guest image. No additional build targets are needed.
`prepare_binaries.sh` locates this package relative to the checkout root.

After building:

```bash
cd example
./0-setup.sh
```

Follow [README.md](README.md) to start the stand.

The scripts use Bash, Python 3 (for configuration generation and `copy_volume.py`), OpenSSL, GNU coreutils,
and GNU getopt. NBD attachment requires `sudo` and the kernel `nbd` module;
QEMU requires KVM. Setup uses sparse files; their physical disk usage grows as
the stand writes data.
