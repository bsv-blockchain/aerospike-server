/*
 * test_module.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Tests for mod-teranode module apply_record dispatch and ownership behavior.
 */

#include <aerospike/mod_teranode.h>
#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_result.h>
#include "test_framework.h"
#include "mock_record.h"

TEST(module_apply_record_set_locked_success)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_udf_context udf_ctx = {
        .as = as_ctx,
        .timer = NULL
    };

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));

    as_result result;
    as_result_init(&result);

    // Mirror UDF runtime contract (udf.c reserves record before module call).
    as_val_reserve((as_val*)rec);

    int rv = as_module_apply_record(&mod_teranode, &udf_ctx, "teranode", "setLocked",
            rec, (as_list*)args, &result);

    ASSERT_EQ(rv, 0);
    ASSERT_TRUE(result.is_success);
    ASSERT_NOT_NULL(result.value);

    as_map* response = as_map_fromval(result.value);
    ASSERT_NOT_NULL(response);

    as_string status_key;
    as_string_init(&status_key, "status", false);
    as_val* status_val = as_map_get(response, (as_val*)&status_key);
    ASSERT_NOT_NULL(status_val);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status_val)), STATUS_OK);

    as_val* locked_val = as_rec_get(rec, BIN_LOCKED);
    ASSERT_NOT_NULL(locked_val);
    ASSERT_TRUE(as_boolean_get(as_boolean_fromval(locked_val)));

    as_result_destroy(&result);
    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(module_apply_record_unknown_function)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 1);

    as_udf_context udf_ctx = {
        .as = as_ctx,
        .timer = NULL
    };

    as_arraylist* args = as_arraylist_new(0, 0);

    as_result result;
    as_result_init(&result);

    // Mirror UDF runtime contract (udf.c reserves record before module call).
    as_val_reserve((as_val*)rec);

    int rv = as_module_apply_record(&mod_teranode, &udf_ctx, "teranode", "doesNotExist",
            rec, (as_list*)args, &result);

    ASSERT_NEQ(rv, 0);
    ASSERT_FALSE(result.is_success);
    ASSERT_NOT_NULL(result.value);
    ASSERT_TRUE(as_val_type(result.value) == AS_STRING);

    const char* msg = as_string_get(as_string_fromval(result.value));
    ASSERT_NOT_NULL(msg);
    ASSERT_TRUE(strstr(msg, "unknown function") != NULL);

    as_result_destroy(&result);
    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(module_apply_record_null_function)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 1);

    as_udf_context udf_ctx = {
        .as = as_ctx,
        .timer = NULL
    };

    as_arraylist* args = as_arraylist_new(0, 0);

    as_result result;
    as_result_init(&result);

    // Mirror UDF runtime contract (udf.c reserves record before module call).
    as_val_reserve((as_val*)rec);

    int rv = as_module_apply_record(&mod_teranode, &udf_ctx, "teranode", NULL,
            rec, (as_list*)args, &result);

    ASSERT_NEQ(rv, 0);
    ASSERT_FALSE(result.is_success);
    ASSERT_NOT_NULL(result.value);
    ASSERT_TRUE(as_val_type(result.value) == AS_STRING);

    const char* msg = as_string_get(as_string_fromval(result.value));
    ASSERT_STR_EQ(msg, "function name required");

    as_result_destroy(&result);
    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(module_apply_record_caller_retains_record_ownership)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 2);

    as_udf_context udf_ctx = {
        .as = as_ctx,
        .timer = NULL
    };

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));

    as_result result;
    as_result_init(&result);

    // Mirror UDF runtime contract (udf.c reserves record before module call).
    as_val_reserve((as_val*)rec);

    int rv = as_module_apply_record(&mod_teranode, &udf_ctx, "teranode", "setLocked",
            rec, (as_list*)args, &result);
    ASSERT_EQ(rv, 0);
    ASSERT_TRUE(result.is_success);

    // Destroy call artifacts first.
    as_result_destroy(&result);
    as_arraylist_destroy(args);

    // Record must remain valid for caller cleanup and subsequent reads.
    as_val* spent_val = as_rec_get(rec, BIN_SPENT_UTXOS);
    ASSERT_NOT_NULL(spent_val);
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 0);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

void run_module_tests(void)
{
    printf("\n=== Module Dispatch/Ownership Tests ===\n");

    RUN_TEST(module_apply_record_set_locked_success);
    RUN_TEST(module_apply_record_unknown_function);
    RUN_TEST(module_apply_record_null_function);
    RUN_TEST(module_apply_record_caller_retains_record_ownership);
}
