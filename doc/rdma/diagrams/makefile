# see https://github.com/mermaid-js/mermaid-cli for installation instructions

all: rw_sequence.svg client_endpoint_states.svg

%.svg: %.mmd makefile
	mmdc -i $< -o $@

.PHONY: all
