GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    asm.go
    constants.go
    doc.go
    instructions.go
    setter.go
    vm.go
    vm_instructions.go
)

END()

RECURSE(
    # gotest
)
