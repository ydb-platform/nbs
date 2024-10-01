set -eu
./kikimr -s grpc://my.host1:2135 admin bs config invoke --proto-file cfg/DefineBox.txt
./kikimr -s grpc://my.host1:2135 admin bs config invoke --proto-file cfg/DefineStoragePools.txt
