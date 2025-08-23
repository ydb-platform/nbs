#!/bin/bash
# Test ls file type indicators - -F and -p options

desc="ls -F and -p options append indicators to filenames based on file type"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..47"

n0=`namegen`
n1=`namegen`
n2=`namegen`

# ls -F appends / to directories
expect 0 mkdir ${n0} 0755
output=`ls -F 2>&1 | grep "${n0}/" | wc -l`
test_check "$output" -eq 1
expect 0 rmdir ${n0}

# ls -F appends * to executable files
expect 0 create ${n0} 0755
output=`ls -F 2>&1 | grep "${n0}\*" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# ls -F appends @ to symbolic links
expect 0 create ${n0} 0644
expect 0 symlink ${n0} ${n1}
output=`ls -F 2>&1 | grep "${n1}@" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n1}
expect 0 unlink ${n0}

# ls -F appends | to FIFOs TODO: mkfifo fails
# expect 0 mkfifo ${n0} 0644
# output=`ls -F 2>&1 | grep "${n0}|" | wc -l`
# test_check "$output" -eq 1
# expect 0 unlink ${n0}

# ls -F shows no indicator for regular non-executable files
expect 0 create ${n0} 0644
output=`ls -F 2>&1 | grep "^${n0}$" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# ls -F with multiple file types
expect 0 create regular 0644
expect 0 create executable 0755
expect 0 mkdir directory 0755
output=`ls -F 2>&1`
# Check each has appropriate indicator
echo "$output" | grep "^regular" >/dev/null && \
echo "$output" | grep "executable\*" >/dev/null && \
echo "$output" | grep "directory/" >/dev/null && \
test_check "$?" -eq 0
expect 0 unlink regular
expect 0 unlink executable
expect 0 rmdir directory

# ls -p appends / to directories
expect 0 mkdir ${n0} 0755
output=`ls -p 2>&1 | grep "${n0}/" | wc -l`
test_check "$output" -eq 1
expect 0 rmdir ${n0}

# ls -p does not append / to regular files
expect 0 create ${n0} 0644
output=`ls -p 2>&1 | grep "^${n0}$" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# ls -p does not append / to symlinks
expect 0 create ${n0} 0644
expect 0 symlink ${n0} ${n1}
output=`ls -p 2>&1 | grep "^${n1}$" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n1}
expect 0 unlink ${n0}

# ls -p with symlink to directory
expect 0 mkdir ${n0} 0755
expect 0 symlink ${n0} ${n1}
output=`ls -p 2>&1 | grep "^${n1}$" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n1}
expect 0 rmdir ${n0}

# ls -F with -H option (don't follow symlinks on command line)
expect 0 mkdir ${n0} 0755
expect 0 symlink ${n0} ${n1}
output=`ls -FH ${n1} 2>&1`
# Should list contents, not show @ indicator
test_check -z "$output"
expect 0 unlink ${n1}
expect 0 rmdir ${n0}

# ls -F with -L option (follow all symlinks)
expect 0 mkdir ${n0} 0755
expect 0 symlink ${n0} ${n1}
output=`ls -FL 2>&1 | grep "${n1}" | wc -l`
# Should treat symlink as directory
test_check "$output" -eq 1
expect 0 unlink ${n1}
expect 0 rmdir ${n0}
