# Build static fio binary

\# Build from sources
```
build-fio --src 'src-dir' [--deps]
```

\# Build from github
```
build-fio --co --git-tag v6.0.0-rc4 [--deps]
```

# Update fio used in tests

\# Upload fio to sandbox
```
ya upload --ttl inf -a linux -d 'fio static build' fio-static.tgz
```

Update resource id and list of files from 'tar --list -f fio-static.tgz' in
```
./tools/testing/fio/bin/ya.make
```
