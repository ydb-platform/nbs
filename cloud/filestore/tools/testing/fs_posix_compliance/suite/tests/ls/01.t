#!/bin/bash
# Test ls -a and -A options - show hidden files

desc="ls -a and -A options handle hidden files correctly"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..55"

n0=`namegen`
n1=`namegen`

cdir=`pwd`

# ls -a shows hidden files
expect 0 create ".hidden1" 0644
expect 0 create ".hidden2" 0644
expect 0 create "visible" 0644
output=`ls -a 2>&1 | grep -E "^\.hidden" | wc -l`
test_check "$output" -eq 2
expect 0 unlink ".hidden1"
expect 0 unlink ".hidden2"
expect 0 unlink "visible"

# ls -a in empty directory shows only . and ..
expect 0 mkdir ${n0} 0755
cd ${n0}
output=`ls -a 2>&1 | wc -l`
test_check "$output" -eq 2
cd ${cdir}
expect 0 rmdir ${n0}

# ls -a sorts all entries including hidden ones
expect 0 create ".aaa" 0644
expect 0 create "bbb" 0644
expect 0 create ".ccc" 0644
expect 0 create "ddd" 0644
output=`ls -a 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = ". .. .aaa .ccc bbb ddd"
expect 0 unlink ".aaa"
expect 0 unlink "bbb"
expect 0 unlink ".ccc"
expect 0 unlink "ddd"

# ls -a with directory argument
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/.hidden 0644
expect 0 create ${n0}/visible 0644
output=`ls -a ${n0} 2>&1 | grep -E "^\.|^visible" | wc -l`
test_check "$output" -eq 4
expect 0 unlink ${n0}/.hidden
expect 0 unlink ${n0}/visible
expect 0 rmdir ${n0}

# ls -A shows hidden files
expect 0 create ".hidden1" 0644
expect 0 create ".hidden2" 0644
expect 0 create "visible" 0644
output=`ls -A 2>&1 | grep -E "^\.hidden" | wc -l`
test_check "$output" -eq 2
expect 0 unlink ".hidden1"
expect 0 unlink ".hidden2"
expect 0 unlink "visible"

# ls -A in empty directory shows nothing
expect 0 mkdir ${n0} 0755
cd ${n0}
output=`ls -A 2>&1`
test_check -z "$output"
cd ${cdir}
expect 0 rmdir ${n0}

# ls -A sorts entries properly
expect 0 create ".aaa" 0644
expect 0 create "bbb" 0644
expect 0 create ".ccc" 0644
expect 0 create "ddd" 0644
output=`ls -A 2>&1 | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = ".aaa .ccc bbb ddd"
expect 0 unlink ".aaa"
expect 0 unlink "bbb"
expect 0 unlink ".ccc"
expect 0 unlink "ddd"

# Default ls does not show hidden files
expect 0 create ".hidden" 0644
expect 0 create "visible" 0644
output=`ls 2>&1`
test_check "$output" = "visible"
expect 0 unlink ".hidden"
expect 0 unlink "visible"

# Hidden files starting with multiple dots
expect 0 create "..hidden" 0644
expect 0 create "...hidden" 0644
output=`ls -a 2>&1 | grep -E "^\.\.+hidden" | wc -l`
test_check "$output" -eq 2
expect 0 unlink "..hidden"
expect 0 unlink "...hidden"
