set -eu
./ydbd --server=grpc://localhost:9001 db schema execute dynamic/BindRootStorageRequest-Root.txt
