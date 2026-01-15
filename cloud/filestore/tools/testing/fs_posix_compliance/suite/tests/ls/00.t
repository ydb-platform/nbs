#!/bin/bash
# Test basic ls functionality - listing files and directories

desc="ls lists directory contents"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..57"

n0=`namegen`
n1=`namegen`
n2=`namegen`
n3=`namegen`

cdir=`pwd`

# ls on empty directory shows nothing
expect 0 mkdir ${n0} 0755
cd ${n0}
output=`ls 2>&1`
test_check -z "$output"
cd ${cdir}
expect 0 rmdir ${n0}

# ls lists regular files
expect 0 create ${n1} 0644
output=`ls 2>&1`
test_check "$output" = "${n1}"
expect 0 unlink ${n1}

# ls lists directories
expect 0 mkdir ${n1} 0755
output=`ls 2>&1`
test_check "$output" = "${n1}"
expect 0 rmdir ${n1}

# ls lists multiple files sorted alphabetically
expect 0 create "bbb" 0644
expect 0 create "aaa" 0644
expect 0 create "ccc" 0644
output=`ls 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "aaa bbb ccc"
expect 0 unlink "aaa"
expect 0 unlink "bbb"
expect 0 unlink "ccc"

# ls does not list hidden files by default
expect 0 create ".hidden" 0644
expect 0 create "visible" 0644
output=`ls 2>&1`
test_check "$output" = "visible"
expect 0 unlink ".hidden"
expect 0 unlink "visible"

# ls on non-existent file returns error
ls ${n1} 2>/dev/null
test_check "$?" -ne 0

# ls lists files within a directory when directory is specified
expect 0 mkdir ${n1} 0755
expect 0 create ${n1}/file1 0644
expect 0 create ${n1}/file2 0644
output=`ls ${n1} 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "file1 file2"
expect 0 unlink ${n1}/file1
expect 0 unlink ${n1}/file2
expect 0 rmdir ${n1}

# ls with multiple directory arguments
expect 0 mkdir ${n1} 0755
expect 0 mkdir ${n2} 0755
expect 0 create ${n1}/file1 0644
expect 0 create ${n2}/file2 0644
output=`ls ${n1} ${n2} 2>&1 | grep -E "^${n1}:|^${n2}:" | wc -l`
test_check "$output" -eq 2
expect 0 unlink ${n1}/file1
expect 0 unlink ${n2}/file2
expect 0 rmdir ${n1}
expect 0 rmdir ${n2}

# ls lists symbolic links
expect 0 create ${n1} 0644
expect 0 symlink ${n1} ${n2}
output=`ls 2>&1 | grep ${n2} | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n2}
expect 0 unlink ${n1}

# ls lists FIFOs
expect 0 mkfifo ${n1} 0644
output=`ls 2>&1`
test_check "$output" = "${n1}"
expect 0 unlink ${n1}

# ls on file in non-existent directory returns error
ls nonexistent/file 2>/dev/null
test_check "$?" -ne 0

# ls continues after encountering errors
expect 0 create ${n0} 0644
output=`ls nonexistent ${n0} 2>/dev/null | grep ${n0} | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# ls handles very long pathnames
expect 0 create ${name255} 0644
output=`ls ${name255} 2>&1`
test_check "$output" = "${name255}"
expect 0 unlink ${name255}

# ls with null pathname returns error
ls "" 2>/dev/null
test_check "$?" -ne 0

# ls exit status is >0 on any error
expect 0 create ${n0} 0644
ls ${n0} nonexistent >/dev/null 2>&1
test_check "$?" -gt 0
expect 0 unlink ${n0}
