RECURSE(
    bin
    build
    build-image
    image
    lib
)

# no need to make it except explicitly
# but arcadia wants it to be reachable
RECURSE_FOR_TESTS(
    win-image
)
