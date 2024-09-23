set -eu
./kikimr -s grpc://my.host1:2135 admin console execute --domain=my_cluster --retry=10 cfg/Configure-my_cluster.txt
