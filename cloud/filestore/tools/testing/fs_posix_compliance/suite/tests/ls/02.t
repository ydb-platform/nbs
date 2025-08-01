#!/bin/sh
# Test ls -d option - list directories themselves, not their contents

desc="ls -d lists directories themselves, not their contents"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..27"

n0=`namegen`
n1=`namegen`
n2=`namegen`

n0="dira${n0}"
n1="dirb${n1}"

# ls -d on directory shows directory name, not contents
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/file1 0644
expect 0 create ${n0}/file2 0644
output=`ls -d ${n0} 2>&1`
test_check "$output" = "${n0}"
expect 0 unlink ${n0}/file1
expect 0 unlink ${n0}/file2
expect 0 rmdir ${n0}

# ls -d . shows current directory
output=`ls -d . 2>&1`
test_check "$output" = "."

# ls -d on multiple directories
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n0}/file1 0644
expect 0 create ${n1}/file2 0644
output=`ls -d ${n0} ${n1} 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "${n0} ${n1}"
expect 0 unlink ${n0}/file1
expect 0 unlink ${n1}/file2
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}

# ls -d on regular file shows the file
expect 0 create ${n0} 0644
output=`ls -d ${n0} 2>&1`
test_check "$output" = "${n0}"
expect 0 unlink ${n0}

# ls -d with wildcards
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n0}/file1 0644
output=`ls -d dir* 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "${n0} ${n1}"
expect 0 unlink ${n0}/file1 
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}
