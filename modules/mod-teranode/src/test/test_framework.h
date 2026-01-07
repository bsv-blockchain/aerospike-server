/*
 * test_framework.h
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Simple test framework for mod-teranode.
 */

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

//==========================================================
// Test macros.
//

static int g_test_count = 0;
static int g_test_passed = 0;
static int g_test_failed = 0;
static int g_current_test_failed = 0;

#define TEST_START() do { \
    g_test_count = 0; \
    g_test_passed = 0; \
    g_test_failed = 0; \
    printf("\n=== Starting Tests ===\n\n"); \
} while(0)

#define TEST(name) \
    static void test_##name(void); \
    static void run_test_##name(void) { \
        g_test_count++; \
        g_current_test_failed = 0; \
        printf("  [%d] Testing %s... ", g_test_count, #name); \
        fflush(stdout); \
        test_##name(); \
        if (g_current_test_failed == 0) { \
            printf("✅ PASS\n"); \
            g_test_passed++; \
        } \
    } \
    static void test_##name(void)

#define RUN_TEST(name) run_test_##name()

#define TEST_SUMMARY() do { \
    printf("\n=== Test Summary ===\n"); \
    printf("Total: %d\n", g_test_count); \
    printf("Passed: %d\n", g_test_passed); \
    printf("Failed: %d\n", g_test_failed); \
    if (g_test_failed == 0) { \
        printf("\n✅ All tests passed!\n\n"); \
    } else { \
        printf("\n❌ %d test(s) failed!\n\n", g_test_failed); \
        exit(1); \
    } \
} while(0)

// Assertion macros
#define ASSERT(condition, message) do { \
    if (!(condition)) { \
        printf("\n❌ FAIL: %s\n", message); \
        printf("   Line: %d\n", __LINE__); \
        g_test_failed++; \
        g_current_test_failed = 1; \
        return; \
    } \
} while(0)

#define ASSERT_TRUE(condition) ASSERT(condition, #condition " is not true")
#define ASSERT_FALSE(condition) ASSERT(!(condition), #condition " is not false")
#define ASSERT_NULL(ptr) ASSERT((ptr) == NULL, #ptr " is not NULL")
#define ASSERT_NOT_NULL(ptr) ASSERT((ptr) != NULL, #ptr " is NULL")
#define ASSERT_EQ(a, b) ASSERT((a) == (b), #a " != " #b)
#define ASSERT_NEQ(a, b) ASSERT((a) != (b), #a " == " #b)
#define ASSERT_STR_EQ(a, b) ASSERT(strcmp(a, b) == 0, #a " != " #b)
#define ASSERT_BYTES_EQ(a, b, len) ASSERT(memcmp(a, b, len) == 0, "bytes not equal")

#ifdef __cplusplus
}
#endif
