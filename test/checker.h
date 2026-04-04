#pragma once

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ANSI Color Codes
#define COLOR_RED "\x1b[31m"
#define COLOR_GREEN "\x1b[32m"
#define COLOR_YELLOW "\x1b[33m"
#define COLOR_BLUE "\x1b[34m"
#define COLOR_RESET "\x1b[0m"

extern uint32_t total_test;
extern uint32_t failed_test;

#define EXPECT_EQ(actual, expected, label)                                     \
    do {                                                                       \
        total_test++;                                                          \
        if ((actual) == (expected)) {                                          \
            printf(COLOR_GREEN "[SUCCESS] " COLOR_RESET "%s\n", label);        \
        } else {                                                               \
            failed_test++;                                                     \
            printf(COLOR_RED "[ERROR]   " COLOR_RESET "%s\n", label);          \
            printf("          Expected: %d, Got: %d\n", (uint32_t) (expected), \
                   (uint32_t) (actual));                                       \
            printf("          Location: %s:%d\n", __FILE__, __LINE__);         \
        }                                                                      \
    } while (0)

#define EXPECT_COND(cond, label)                                        \
    do {                                                                \
        total_test++;                                                   \
        if (!!(cond)) {                                                 \
            printf(COLOR_GREEN "[SUCCESS] " COLOR_RESET "%s\n", label); \
        } else {                                                        \
            failed_test++;                                              \
            printf(COLOR_RED "[ERROR]   " COLOR_RESET "%s\n", label);   \
            printf("          Condition failed: [%s]\n", #cond);        \
            printf("          Location: %s:%d\n", __FILE__, __LINE__);  \
        }                                                               \
    } while (0)

#define EXPECT_STR_EQ(actual, expected, label)                          \
    do {                                                                \
        total_test++;                                                   \
        if (!!(actual && !!(expected) &&                                \
               strncmp((actual), (expected), strlen(expected)) == 0)) { \
            printf(COLOR_GREEN "[SUCCESS] " COLOR_RESET "%s\n", label); \
        } else {                                                        \
            failed_test++;                                              \
            printf(COLOR_RED "[ERROR]   " COLOR_RESET "%s\n", label);   \
            printf("          Expected: [%s], Got: [%s]\n", (expected), \
                   (actual));                                           \
            printf("          Location: %s:%d\n", __FILE__, __LINE__);  \
        }                                                               \
    } while (0)

#define TEST_SUMMARY()                                                        \
    do {                                                                      \
        printf("\n" COLOR_BLUE "--- Test Report Summary ---" COLOR_RESET      \
               "\n");                                                         \
        printf("Passed: " COLOR_GREEN "%u" COLOR_RESET " / %u\n",             \
               (total_test - failed_test), total_test);                       \
        if (failed_test == 0) {                                               \
            printf(COLOR_GREEN "STATUS: ALL PASSED" COLOR_RESET "\n\n");      \
            return 0;                                                         \
        } else {                                                              \
            printf(COLOR_RED "STATUS: FAILED (%u errors)" COLOR_RESET "\n\n", \
                   failed_test);                                              \
            return 1; /* This is what triggers the CI failure */              \
        }                                                                     \
    } while (0)
