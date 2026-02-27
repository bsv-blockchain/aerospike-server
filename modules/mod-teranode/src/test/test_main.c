/*
 * test_main.c
 *
 * Copyright (C) 2026 BSV Association.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
 *
 * Main test runner for mod-teranode.
 */

#include <stdio.h>
#include "test_framework.h"

// External test suite runners
extern void run_helper_tests(void);
extern void run_spend_tests(void);
extern void run_freeze_tests(void);
extern void run_state_management_tests(void);
extern void run_additional_spend_tests(void);
extern void run_additional_state_management_tests(void);
extern void run_comprehensive_tests(void);
extern void run_module_tests(void);

int main(void)
{
    printf("\n");
    printf("╔═══════════════════════════════════════════════════════╗\n");
    printf("║     TERANODE Native Module - Comprehensive Tests     ║\n");
    printf("╚═══════════════════════════════════════════════════════╝\n");

    TEST_START();

    // Run all test suites
    run_helper_tests();
    run_spend_tests();
    run_freeze_tests();
    run_state_management_tests();
    run_additional_spend_tests();
    run_additional_state_management_tests();
    run_comprehensive_tests();
    run_module_tests();

    TEST_SUMMARY();

    return (g_test_failed == 0) ? 0 : 1;
}
