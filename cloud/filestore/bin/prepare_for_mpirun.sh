#!/usr/bin/env bash

# set -x

NODE_FILE="${1:-mpi_nodes.txt}"
NODE_NAME_PREFIX="${2:-fsclient}"
IOR_REPO="https://github.com/hpc/ior.git"
SSH_USER="${3:-root}"

git clone "${IOR_REPO}" &&
cd ior &&
./bootstrap &&
./configure &&
make &&
echo "built ior"

if [ ! -f $NODE_FILE ]; then
    echo "node file ${NODE_FILE} does not exist"
    exit 1
fi

HOSTS_FILE="${NODE_NAME_PREFIX}_hosts.txt"
rm -f "$HOSTS_FILE"

NODES=$(cat $NODE_FILE)

i=1
while read addr
do
    echo "${addr} ${NODE_NAME_PREFIX}${i}" >> "$HOSTS_FILE"
    i=$((i + 1))
done <<< "$NODES"

i=1
while read addr
do
    node="[${addr}]"
    node_host="${NODE_NAME_PREFIX}${i}"
    scp "$HOSTS_FILE" "${SSH_USER}@${node}:~/" &&
    ssh -n "${SSH_USER}@$addr" "cat ${HOSTS_FILE} >> /etc/hosts" &&
    echo "updated hosts file @ ${addr}"

    scp ior/src/ior "${SSH_USER}@${node}:~/" &&
    echo "copied ior to ${addr}"

    ssh -n "${SSH_USER}@$addr" "hostname ${node_host}" &&
    echo "updated hostname @ ${addr}"

    ssh -n "${SSH_USER}@$addr" "apt install openmpi-bin openmpi-common libopenmpi-dev -y" &&
    echo "installed mpirun @ ${addr}"

    i=$((i + 1))
done <<< "$NODES"

read addr <<< "$NODES"
i=1
j=1
while read other_addr
do
    other_node_host="${NODE_NAME_PREFIX}${j}"
    ssh -n "${SSH_USER}@$addr" "ssh -o StrictHostKeyChecking=no ${other_node_host} 'echo a'" &&
    echo "ssh ${addr} -> ${other_addr} succeeded"
    j=$((j + 1))
done <<< "$NODES"

# now you can call mpirun from fsclient1 in the following way:
# export NODES="fsclient1:100,fsclient2:100,fsclient3:100,fsclient4:100,fsclient5:100,fsclient6:100,fsclient7:100,fsclient8:100,fsclient9:100,fsclient10:100"
# mpirun --mca routed direct -H $NODES -np 200 /root/ior -o /root/mnt/ior_file -t 1m -b 32m -s 16 -F -C
