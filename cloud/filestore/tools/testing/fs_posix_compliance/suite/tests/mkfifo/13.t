#!/bin/bash
# Test pipe functionality

desc="users should be able to use named pipes to send data between processes"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..6"

n0=`namegen`
n1=`namegen`
n2=`namegen`

expect 0 mkdir ${n1} 0755
cdir=`pwd`
cd ${n1}

expect 0 mkfifo ${n0} 0644

# start background reader to capture fifo input
{
    read READ_DATA < "$n0"
    echo "$READ_DATA" > "$n2"
} &
# write test string to fifo
TEST_STRING="fifo_test_message_123"
echo "$TEST_STRING" > "$n0"
# wait for the reader (a single child) to finish
wait
# read captured data
read_output=$(cat "$n2")
# compare read data with test string
test_check_quoted "$TEST_STRING" = "$read_output"

# cleanup
expect 0 unlink ${n0}
expect 0 unlink ${n2}
cd "$cdir"
expect 0 rmdir ${n1}
