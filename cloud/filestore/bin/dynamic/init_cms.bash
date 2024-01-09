set -eu
./ydbd -s grpc://localhost:9001 admin console execute --domain=Root --retry=10 dynamic/Configure-Root.txt
