#!/usr/bin/env bash

# Copyright 2024 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

function usage {
  local script="$(basename $0)"

  echo >&2 "Usage: ${script} [-g <maximum go directive>]

This script should be run at the root of a module.

-g <maximum go directive>
  Compare the go directive in the local working copy's go.mod
  to the specified maximum version it can be. Versions provided
  here are of the form 1.x.y, without the 'go' prefix.

Examples:
  ${script} -g 1.20
  ${script} -g 1.21.6
"
  exit 1
}

max=""
while getopts g: o
do case "$o" in
  g)    max="$OPTARG";;
  [?])  usage;;
  esac
done

# If max is empty, print usage and error
if [[ -z "${max}" ]]; then
  usage;
fi

# Don't specify the version with the go prefix, just 1.x.y will do.
if ! printf '%s\n' "${max}" | grep -v -q "^go"; then
  usage;
fi

current=$(grep '^go [1-9]*' go.mod | cut -d ' ' -f2)
if [[ -z "${current}" ]]; then
  echo >&2 "FAIL: could not get value of go directive from go.mod"
  exit 1
fi

if ! printf '%s\n' "${current}" "${max}" | sort --check=silent --version-sort; then
    echo >&2 "FAIL: current Go directive ${current} is greater than ${max}"
    exit 1
fi
