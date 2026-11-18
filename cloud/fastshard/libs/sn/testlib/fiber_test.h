#pragma once

#include <silk/fibers/fiber.h>

#include <gtest/gtest.h>

// A gtest case whose body runs inside a silk fiber. The binary must register
// the environment from silk_env.h once.
#define FIBER_TEST(suite, name)                                                \
    void suite##_##name##_Body();                                              \
    TEST(suite, name)                                                          \
    {                                                                          \
        const int r = silk::FiberScheduler::run(                               \
            +[](int*) noexcept -> int                                          \
            {                                                                  \
                suite##_##name##_Body();                                       \
                return 0;                                                      \
            },                                                                 \
            0);                                                                \
        EXPECT_EQ(0, r);                                                       \
    }                                                                          \
    void suite##_##name##_Body()
