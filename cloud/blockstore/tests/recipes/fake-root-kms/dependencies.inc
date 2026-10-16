DEPENDS(
    cloud/blockstore/tools/testing/fake-root-kms
)

PEERDIR(
)

DATA(
    arcadia/cloud/blockstore/tests/recipes/fake-root-kms/certs/ca.crt
    arcadia/cloud/blockstore/tests/recipes/fake-root-kms/certs/server.crt
    arcadia/cloud/blockstore/tests/recipes/fake-root-kms/certs/server.key
    arcadia/cloud/blockstore/tests/recipes/fake-root-kms/certs/client.crt
    arcadia/cloud/blockstore/tests/recipes/fake-root-kms/certs/client.key
    arcadia/cloud/blockstore/tests/recipes/fake-root-kms/certs/nbs.key
)
