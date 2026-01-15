DEPENDS(
    cloud/filestore/apps/server
    cloud/filestore/tests/recipes/service-local
)

USE_RECIPE(
    cloud/filestore/tests/recipes/service-local/service-local-recipe --service=local --file-io=${SERVICE_LOCAL_FILE_IO}
)
