#!/usr/bin/env bash

# Using lowest available OS version (jammy) to avoid GLIBC incompatibility
# https://packages.ubuntu.com/jammy/util-linux                                                                                                               

# To get current installed urls:
# apt-get download --print-uris util-linux                                                                                                                   
                                                                                                                                                             
NAME=util-linux_2.37.2-4ubuntu3.5                                                                                                                            
curl -O http://mirror.nebiusinfra.net/ubuntu/pool/main/u/util-linux/${NAME}_amd64.deb                                                                        
curl -O http://mirror.nebiusinfra.net/ubuntu/pool/main/u/util-linux/${NAME}_arm64.deb                                                                        
dpkg-deb --fsys-tarfile ${NAME}_amd64.deb  | tar -xvOf - ./usr/bin/flock > flock                                                                             
chmod +x flock                                                                                                                                               
tar -czvf flock_amd64.tgz flock                                                                                                                              
rm flock                                                                                                                                                     
dpkg-deb --fsys-tarfile ${NAME}_arm64.deb  | tar -xvOf - ./usr/bin/flock > flock                                                                             
chmod +x flock                                                                                                                                               
tar -czvf flock_arm64.tgz flock                                                                                                                              
