#!/bin/bash

# see https://github.com/mermaid-js/mermaid-cli for installation instructions

set -x

mmdc -i rw_sequence.mmd -o rw_sequence.svg
