#!/bin/bash

desc="ENOENT: A component of dirname does not name an existing directory or dirname is an empty string"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..1"

n0=`namegen`

# ENOENT: Directory does not exist
expect ENOENT opendir ${n0}
