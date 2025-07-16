#!/bin/sh
# Test ls permission and access checks

desc="ls respects file and directory permissions"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..22"

n0=`namegen`
n1=`namegen`
n2=`namegen`

# ls can list directory with read permission
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/file 0644
output=`ls ${n0} 2>&1`
test_check "$output" = "file"
expect 0 unlink ${n0}/file
expect 0 rmdir ${n0}

# ls cannot list directory without read permission
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/file 0644
expect 0 chmod ${n0} 0111
ls ${n0} 2>/dev/null
test_check "$?" -ne 0
expect 0 chmod ${n0} 0755
expect 0 unlink ${n0}/file
expect 0 rmdir ${n0}

# ls can stat files in directory without read but with execute permission
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/file 0644
expect 0 chmod ${n0} 0111
output=`ls ${n0}/file 2>&1`
test_check "$output" = "${n0}/file"
expect 0 chmod ${n0} 0755
expect 0 unlink ${n0}/file
expect 0 rmdir ${n0}

# ls shows files even if not readable (but can stat them)
expect 0 create ${n0} 0000
output=`ls ${n0} 2>&1`
test_check "$output" = "${n0}"
expect 0 unlink ${n0}
