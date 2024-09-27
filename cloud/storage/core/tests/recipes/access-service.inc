DEPENDS(
    cloud/storage/core/tests/recipes/access-service
)

IF (TEST_TOKEN)
    USE_RECIPE(
        cloud/storage/core/tests/recipes/access-service/access-service-recipe
        --token $TEST_TOKEN
    )
ELSE()
     USE_RECIPE(
        cloud/storage/core/tests/recipes/access-service/access-service-recipe
    )
ENDIF()

