#!/bin/bash

desc="setgid bit behavior"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..27"

n0=`namegen`
n1=`namegen`

expect 0 mkdir ${n0} 0755
expect 0755 stat ${n0} mode
uid=`stat -c %u ${n0} 2>&1`
expect 0 chown ${n0} ${uid} 65535
expect 0 chmod ${n0} 02755
expect 02755 stat ${n0} mode

cd ${n0}

# parent setgid flag should be propagated to the new directory
expect 0 mkdir ${n1} 0755
expect 02755 stat ${n1} mode
expect 0 rmdir ${n1}

for type in regular dir fifo block char socket; do
	create_file ${type} ${n1}
    expect 65535 stat ${n1} gid

	if [ "${type}" = "dir" ]; then
		expect 0 rmdir ${n1}
	else
		expect 0 unlink ${n1}
	fi
done

cd ..
expect 0 rmdir ${n0}
