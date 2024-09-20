./kikimr -s grpc://my.host1:2135 admin console execute --domain=my_cluster --retry=10 cfg/CreateTenant-1.txt
./kikimr -s grpc://my.host1:2135 admin console execute --domain=my_cluster --retry=10 cfg/CreateTenant-2.txt
exit 0
