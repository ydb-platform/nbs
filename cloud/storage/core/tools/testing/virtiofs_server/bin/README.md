Binaries downloaded from https://packages.ubuntu.com/en/noble/virtiofsd with:

```bash
mkdir -p amd64
pushd amd64
curl -O http://archive.ubuntu.com/ubuntu/pool/universe/r/rust-virtiofsd/virtiofsd_1.10.0-1_amd64.deb
dpkg-deb --fsys-tarfile virtiofsd_1.10.0-1_amd64.deb | tar -xvf - --strip-components=3 ./usr/libexec/virtiofsd
mv virtiofsd virtiofsd_1.10.0-1_amd64
popd

mkdir -p arm64
pushd arm64
curl -O http://ports.ubuntu.com/pool/universe/r/rust-virtiofsd/virtiofsd_1.10.0-1_arm64.deb
dpkg-deb --fsys-tarfile virtiofsd_1.10.0-1_arm64.deb | tar -xvf - --strip-components=3 ./usr/libexec/virtiofsd
mv virtiofsd virtiofsd_1.10.0-1_arm64
popd
```
