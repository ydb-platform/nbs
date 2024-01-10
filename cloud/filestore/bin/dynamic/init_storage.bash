set -eu
./ydbd -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineBox.txt
./ydbd -s grpc://localhost:9001 admin bs config invoke --proto-file dynamic/DefineStoragePools.txt
