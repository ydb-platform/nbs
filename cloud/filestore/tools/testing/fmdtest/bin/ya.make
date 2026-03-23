PROGRAM(fmdtest)

SRCS(
    app.cpp
    main.cpp
    options.cpp
)

IF (USE_MPI == "local")
    SRCS(mpi_real.cpp)
    DEFAULT(USE_MPI_CFLAGS "")
    DEFAULT(USE_MPI_LDFLAGS -lmpi)
    GLOBAL_CFLAGS(${USE_MPI_CFLAGS})
    LDFLAGS(${USE_MPI_LDFLAGS})
ELSE()
    SRCS(mpi_stub.cpp)
ENDIF()

PEERDIR(
    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/json
    library/cpp/string_utils/base64

    util
)

END()
