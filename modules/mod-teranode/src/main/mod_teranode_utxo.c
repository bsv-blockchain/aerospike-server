/*
 * mod_teranode_utxo.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * UTXO function implementations.
 * Direct port from teranode.lua to C.
 */

#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "internal.h"

//==========================================================
// Forward declarations and inline helpers.
//

// Helper to get boolean from list
static inline bool get_list_bool(as_list* list, uint32_t index) {
    as_val* val = as_list_get(list, index);
    if (val == NULL) return false;
    as_boolean* b = as_boolean_fromval(val);
    if (b == NULL) return false;
    return as_boolean_get(b);
}

// Helper to get integer from list
static inline int64_t get_list_int64(as_list* list, uint32_t index) {
    as_val* val = as_list_get(list, index);
    if (val == NULL) return 0;
    as_integer* i = as_integer_fromval(val);
    if (i == NULL) return 0;
    return as_integer_get(i);
}

// Helper to get bytes from list
static inline as_bytes* get_list_bytes(as_list* list, uint32_t index) {
    as_val* val = as_list_get(list, index);
    if (val == NULL) return NULL;
    return as_bytes_fromval(val);
}

//==========================================================
// Helper function implementations.
//

bool
utxo_bytes_equal(as_bytes* a, as_bytes* b)
{
    if (a == NULL || b == NULL) {
        return a == b;
    }

    uint32_t size_a = as_bytes_size(a);
    uint32_t size_b = as_bytes_size(b);

    if (size_a != size_b) {
        return false;
    }

    return memcmp(as_bytes_get(a), as_bytes_get(b), size_a) == 0;
}

bool
utxo_is_frozen(as_bytes* spending_data)
{
    if (spending_data == NULL) {
        return false;
    }

    uint32_t size = as_bytes_size(spending_data);
    if (size != SPENDING_DATA_SIZE) {
        return false;
    }

    const uint8_t* data = as_bytes_get(spending_data);
    for (uint32_t i = 0; i < SPENDING_DATA_SIZE; i++) {
        if (data[i] != FROZEN_BYTE) {
            return false;
        }
    }

    return true;
}

as_bytes*
utxo_create_with_spending_data(as_bytes* utxo_hash, as_bytes* spending_data)
{
    if (utxo_hash == NULL) {
        return NULL;
    }

    uint32_t hash_size = as_bytes_size(utxo_hash);
    if (hash_size != UTXO_HASH_SIZE) {
        return NULL;
    }

    uint32_t new_size = (spending_data != NULL) ? FULL_UTXO_SIZE : UTXO_HASH_SIZE;
    as_bytes* new_utxo = as_bytes_new(new_size);

    if (new_utxo == NULL) {
        return NULL;
    }

    // Copy utxo hash
    as_bytes_set(new_utxo, 0, as_bytes_get(utxo_hash), UTXO_HASH_SIZE);

    // Copy spending data if provided
    if (spending_data != NULL) {
        as_bytes_set(new_utxo, UTXO_HASH_SIZE, as_bytes_get(spending_data), SPENDING_DATA_SIZE);
    }

    return new_utxo;
}

int
utxo_get_and_validate(
    as_list* utxos,
    int64_t offset,
    as_bytes* expected_hash,
    as_bytes** out_utxo,
    as_bytes** out_spending_data,
    as_map** out_error_response)
{
    *out_utxo = NULL;
    *out_spending_data = NULL;
    *out_error_response = NULL;

    if (utxos == NULL || expected_hash == NULL) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Invalid parameters");
        return -1;
    }

    // Lua arrays are 1-based, but as_list is 0-based
    as_val* val = as_list_get(utxos, (uint32_t)offset);
    if (val == NULL) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_NOT_FOUND, ERR_UTXO_NOT_FOUND);
        return -1;
    }

    as_bytes* utxo = as_bytes_fromval(val);
    if (utxo == NULL) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
        return -1;
    }

    uint32_t utxo_size = as_bytes_size(utxo);
    if (utxo_size != UTXO_HASH_SIZE && utxo_size != FULL_UTXO_SIZE) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
        return -1;
    }

    // Extract and compare hash (first 32 bytes)
    const uint8_t* utxo_data = as_bytes_get(utxo);
    const uint8_t* expected_data = as_bytes_get(expected_hash);

    if (memcmp(utxo_data, expected_data, UTXO_HASH_SIZE) != 0) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_HASH_MISMATCH, ERR_UTXO_HASH_MISMATCH);
        return -1;
    }

    *out_utxo = utxo;

    // Extract spending data if present
    if (utxo_size == FULL_UTXO_SIZE) {
        // Create a new bytes object for spending data
        as_bytes* spending = as_bytes_new(SPENDING_DATA_SIZE);
        as_bytes_set(spending, 0, utxo_data + UTXO_HASH_SIZE, SPENDING_DATA_SIZE);
        *out_spending_data = spending;
    }

    return 0;
}

as_string*
utxo_spending_data_to_hex(as_bytes* spending_data)
{
    if (spending_data == NULL || as_bytes_size(spending_data) != SPENDING_DATA_SIZE) {
        return NULL;
    }

    // Output: 64 chars (32 bytes reversed) + 8 chars (4 bytes LE) + null
    char hex[73];
    const uint8_t* data = as_bytes_get(spending_data);

    // First 32 bytes reversed (txID)
    int pos = 0;
    for (int i = 31; i >= 0; i--) {
        sprintf(&hex[pos], "%02x", data[i]);
        pos += 2;
    }

    // Next 4 bytes as-is (vin in little-endian)
    for (int i = 32; i < 36; i++) {
        sprintf(&hex[pos], "%02x", data[i]);
        pos += 2;
    }

    hex[72] = '\0';
    // Use strdup to copy string to heap, true flag means as_string will free it
    return as_string_new(strdup(hex), true);
}

// Convert spending data to just txid hex (32 bytes reversed) - for deletedChildren check
static as_string*
utxo_spending_data_to_txid_hex(as_bytes* spending_data)
{
    if (spending_data == NULL || as_bytes_size(spending_data) != SPENDING_DATA_SIZE) {
        return NULL;
    }

    // Output: 64 chars (32 bytes reversed) + null
    char hex[65];
    const uint8_t* data = as_bytes_get(spending_data);

    // First 32 bytes reversed (txID)
    int pos = 0;
    for (int i = 31; i >= 0; i--) {
        sprintf(&hex[pos], "%02x", data[i]);
        pos += 2;
    }

    hex[64] = '\0';
    // Use strdup to copy string to heap, true flag means as_string will free it
    return as_string_new(strdup(hex), true);
}

as_map*
utxo_create_error_response(const char* error_code, const char* message)
{
    as_hashmap* response = as_hashmap_new(4);

    as_hashmap_set(response,
        (as_val*)as_string_new((char*)FIELD_STATUS, false),
        (as_val*)as_string_new((char*)STATUS_ERROR, false));

    as_hashmap_set(response,
        (as_val*)as_string_new((char*)FIELD_ERROR_CODE, false),
        (as_val*)as_string_new((char*)error_code, false));

    // Always strdup message since caller might pass stack-allocated buffer
    as_hashmap_set(response,
        (as_val*)as_string_new((char*)FIELD_MESSAGE, false),
        (as_val*)as_string_new(strdup(message), true));

    return (as_map*)response;
}

as_map*
utxo_create_ok_response(void)
{
    as_hashmap* response = as_hashmap_new(2);

    as_hashmap_set(response,
        (as_val*)as_string_new((char*)FIELD_STATUS, false),
        (as_val*)as_string_new((char*)STATUS_OK, false));

    return (as_map*)response;
}

//==========================================================
// setDeleteAtHeight implementation (internal helper).
//

const char*
utxo_set_delete_at_height_impl(
    as_rec* rec,
    int64_t current_block_height,
    int64_t block_height_retention,
    int64_t* out_child_count)
{
    *out_child_count = 0;

    if (block_height_retention == 0) {
        return "";
    }

    // Check preserveUntil
    as_val* preserve_until_val = as_rec_get(rec, BIN_PRESERVE_UNTIL);
    if (preserve_until_val != NULL && as_val_type(preserve_until_val) != AS_NIL) {
        return "";
    }

    // Get relevant bins
    as_val* block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);
    as_val* total_extra_recs_val = as_rec_get(rec, BIN_TOTAL_EXTRA_RECS);
    as_val* spent_extra_recs_val = as_rec_get(rec, BIN_SPENT_EXTRA_RECS);
    as_val* existing_dah_val = as_rec_get(rec, BIN_DELETE_AT_HEIGHT);
    as_val* conflicting_val = as_rec_get(rec, BIN_CONFLICTING);
    as_val* external_val = as_rec_get(rec, BIN_EXTERNAL);
    as_val* spent_utxos_val = as_rec_get(rec, BIN_SPENT_UTXOS);
    as_val* record_utxos_val = as_rec_get(rec, BIN_RECORD_UTXOS);
    as_val* unmined_since_val = as_rec_get(rec, BIN_UNMINED_SINCE);
    as_val* last_spent_state_val = as_rec_get(rec, BIN_LAST_SPENT_STATE);

    int64_t new_delete_height = current_block_height + block_height_retention;
    int64_t spent_extra_recs = 0;
    bool has_external = (external_val != NULL && as_val_type(external_val) != AS_NIL);

    if (spent_extra_recs_val != NULL && as_val_type(spent_extra_recs_val) == AS_INTEGER) {
        spent_extra_recs = as_integer_get(as_integer_fromval(spent_extra_recs_val));
    }

    // Handle conflicting transactions
    if (conflicting_val != NULL && as_val_type(conflicting_val) == AS_BOOLEAN) {
        if (as_boolean_get(as_boolean_fromval(conflicting_val))) {
            if (existing_dah_val == NULL || as_val_type(existing_dah_val) == AS_NIL) {
                as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)as_integer_new(new_delete_height));
                if (has_external && total_extra_recs_val != NULL) {
                    *out_child_count = as_integer_get(as_integer_fromval(total_extra_recs_val));
                    return SIGNAL_DELETE_AT_HEIGHT_SET;
                }
            }
            return "";
        }
    }

    // Handle pagination records (no totalExtraRecs = child record)
    if (total_extra_recs_val == NULL || as_val_type(total_extra_recs_val) == AS_NIL) {
        // Default nil to NOTALLSPENT
        const char* last_state = SIGNAL_NOT_ALL_SPENT;
        if (last_spent_state_val != NULL && as_val_type(last_spent_state_val) == AS_STRING) {
            last_state = as_string_get(as_string_fromval(last_spent_state_val));
        }

        int64_t spent_utxos = 0;
        int64_t record_utxos = 0;

        if (spent_utxos_val != NULL && as_val_type(spent_utxos_val) == AS_INTEGER) {
            spent_utxos = as_integer_get(as_integer_fromval(spent_utxos_val));
        }
        if (record_utxos_val != NULL && as_val_type(record_utxos_val) == AS_INTEGER) {
            record_utxos = as_integer_get(as_integer_fromval(record_utxos_val));
        }

        const char* current_state = (spent_utxos == record_utxos) ?
            SIGNAL_ALL_SPENT : SIGNAL_NOT_ALL_SPENT;

        if (strcmp(last_state, current_state) != 0) {
            as_rec_set(rec, BIN_LAST_SPENT_STATE, (as_val*)as_string_new((char*)current_state, false));
            return current_state;
        }
        return "";
    }

    // Master record: check all conditions for deletion
    int64_t total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));
    *out_child_count = total_extra_recs;

    int64_t spent_utxos = 0;
    int64_t record_utxos = 0;

    if (spent_utxos_val != NULL && as_val_type(spent_utxos_val) == AS_INTEGER) {
        spent_utxos = as_integer_get(as_integer_fromval(spent_utxos_val));
    }
    if (record_utxos_val != NULL && as_val_type(record_utxos_val) == AS_INTEGER) {
        record_utxos = as_integer_get(as_integer_fromval(record_utxos_val));
    }

    bool all_spent = (total_extra_recs == spent_extra_recs) && (spent_utxos == record_utxos);

    bool has_block_ids = false;
    if (block_ids_val != NULL && as_val_type(block_ids_val) == AS_LIST) {
        has_block_ids = as_list_size(as_list_fromval(block_ids_val)) > 0;
    }

    bool is_on_longest_chain = (unmined_since_val == NULL || as_val_type(unmined_since_val) == AS_NIL);

    if (all_spent && has_block_ids && is_on_longest_chain) {
        int64_t existing_dah = 0;
        if (existing_dah_val != NULL && as_val_type(existing_dah_val) == AS_INTEGER) {
            existing_dah = as_integer_get(as_integer_fromval(existing_dah_val));
        }

        if (existing_dah == 0 || existing_dah < new_delete_height) {
            as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)as_integer_new(new_delete_height));
            if (has_external) {
                return SIGNAL_DELETE_AT_HEIGHT_SET;
            }
        }
    } else if (existing_dah_val != NULL && as_val_type(existing_dah_val) != AS_NIL) {
        as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)&as_nil);
        if (has_external) {
            return SIGNAL_DELETE_AT_HEIGHT_UNSET;
        }
    }

    return "";
}

//==========================================================
// UTXO Function Implementations.
//

as_val*
teranode_spend(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);
    as_bytes* spending_data = get_list_bytes(args, 2);
    bool ignore_conflicting = get_list_bool(args, 3);
    bool ignore_locked = get_list_bool(args, 4);
    int64_t current_block_height = get_list_int64(args, 5);
    int64_t block_height_retention = get_list_int64(args, 6);

    // Build single spend item for spendMulti
    as_hashmap* spend_item = as_hashmap_new(4);
    as_hashmap_set(spend_item,
        (as_val*)as_string_new((char*)"offset", false),
        (as_val*)as_integer_new(offset));
    as_hashmap_set(spend_item,
        (as_val*)as_string_new((char*)"utxoHash", false),
        as_val_reserve((as_val*)utxo_hash));
    as_hashmap_set(spend_item,
        (as_val*)as_string_new((char*)"spendingData", false),
        as_val_reserve((as_val*)spending_data));

    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)spend_item);

    // Build args for spendMulti
    as_arraylist* multi_args = as_arraylist_new(5, 0);
    as_arraylist_append(multi_args, (as_val*)spends);
    as_arraylist_append(multi_args, (as_val*)as_boolean_new(ignore_conflicting));
    as_arraylist_append(multi_args, (as_val*)as_boolean_new(ignore_locked));
    as_arraylist_append_int64(multi_args, current_block_height);
    as_arraylist_append_int64(multi_args, block_height_retention);

    as_val* result = teranode_spend_multi(rec, (as_list*)multi_args);
    as_val_destroy((as_val*)multi_args);
    return result;
}

as_val*
teranode_spend_multi(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    as_hashmap* response = as_hashmap_new(8);

    // Extract arguments
    as_list* spends = as_list_fromval(as_list_get(args, 0));
    bool ignore_conflicting = get_list_bool(args, 1);
    bool ignore_locked = get_list_bool(args, 2);
    int64_t current_block_height = get_list_int64(args, 3);
    int64_t block_height_retention = get_list_int64(args, 4);

    // Check creating flag
    as_val* creating_val = as_rec_get(rec, BIN_CREATING);
    if (creating_val != NULL && as_val_type(creating_val) == AS_BOOLEAN) {
        if (as_boolean_get(as_boolean_fromval(creating_val))) {
            as_val_destroy((as_val*)response);
            return (as_val*)utxo_create_error_response(ERROR_CODE_CREATING, MSG_CREATING);
        }
    }

    // Check conflicting
    if (!ignore_conflicting) {
        as_val* conflicting_val = as_rec_get(rec, BIN_CONFLICTING);
        if (conflicting_val != NULL && as_val_type(conflicting_val) == AS_BOOLEAN) {
            if (as_boolean_get(as_boolean_fromval(conflicting_val))) {
                as_val_destroy((as_val*)response);
                return (as_val*)utxo_create_error_response(ERROR_CODE_CONFLICTING, MSG_CONFLICTING);
            }
        }
    }

    // Check locked
    if (!ignore_locked) {
        as_val* locked_val = as_rec_get(rec, BIN_LOCKED);
        if (locked_val != NULL && as_val_type(locked_val) == AS_BOOLEAN) {
            if (as_boolean_get(as_boolean_fromval(locked_val))) {
                as_val_destroy((as_val*)response);
                return (as_val*)utxo_create_error_response(ERROR_CODE_LOCKED, MSG_LOCKED);
            }
        }
    }

    // Check coinbase spending height
    as_val* spending_height_val = as_rec_get(rec, BIN_SPENDING_HEIGHT);
    if (spending_height_val != NULL && as_val_type(spending_height_val) == AS_INTEGER) {
        int64_t coinbase_spending_height = as_integer_get(as_integer_fromval(spending_height_val));
        if (coinbase_spending_height > 0 && coinbase_spending_height > current_block_height) {
            char msg[256];
            snprintf(msg, sizeof(msg), "%s, spendable in block %lld or greater. Current block height is %lld",
                MSG_COINBASE_IMMATURE, (long long)coinbase_spending_height, (long long)current_block_height);
            as_val_destroy((as_val*)response);
            return (as_val*)utxo_create_error_response(ERROR_CODE_COINBASE_IMMATURE, msg);
        }
    }

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        as_val_destroy((as_val*)response);
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Get block IDs for response
    as_val* block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);

    // Get deleted children map
    as_val* deleted_children_val = as_rec_get(rec, BIN_DELETED_CHILDREN);
    as_map* deleted_children = NULL;
    if (deleted_children_val != NULL && as_val_type(deleted_children_val) == AS_MAP) {
        deleted_children = as_map_fromval(deleted_children_val);
    }

    // Get spendable_in map
    as_val* spendable_in_val = as_rec_get(rec, BIN_UTXO_SPENDABLE_IN);
    as_map* spendable_in = NULL;
    if (spendable_in_val != NULL && as_val_type(spendable_in_val) == AS_MAP) {
        spendable_in = as_map_fromval(spendable_in_val);
    }

    // Process each spend
    as_hashmap* errors = as_hashmap_new(8);
    uint32_t spends_size = as_list_size(spends);

    for (uint32_t i = 0; i < spends_size; i++) {
        as_map* spend_item = as_map_fromval(as_list_get(spends, i));
        if (spend_item == NULL) continue;

        as_string* k_offset = as_string_new((char*)"offset", false);
        as_val* v_offset = as_map_get(spend_item, (as_val*)k_offset);
        int64_t offset = as_integer_get(as_integer_fromval(v_offset));
        as_val_destroy((as_val*)k_offset);

        as_string* k_utxo = as_string_new((char*)"utxoHash", false);
        as_bytes* utxo_hash = as_bytes_fromval(as_map_get(spend_item, (as_val*)k_utxo));
        as_val_destroy((as_val*)k_utxo);

        as_string* k_sp = as_string_new((char*)"spendingData", false);
        as_bytes* spending_data = as_bytes_fromval(as_map_get(spend_item, (as_val*)k_sp));
        as_val_destroy((as_val*)k_sp);

        as_string* k_idx = as_string_new((char*)"idx", false);
        as_val* idx_val = as_map_get(spend_item, (as_val*)k_idx);
        as_val_destroy((as_val*)k_idx);
        int64_t idx = (idx_val != NULL) ? as_integer_get(as_integer_fromval(idx_val)) : i;

        // Validate UTXO
        as_bytes* utxo = NULL;
        as_bytes* existing_spending_data = NULL;
        as_map* error_response = NULL;

        if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
            as_hashmap_set(errors,
                (as_val*)as_integer_new(idx),
                (as_val*)error_response);
            continue;
        }

        // Check spendable_in
        if (spendable_in != NULL) {
            as_integer* k_off = as_integer_new(offset);
            as_val* spendable_val = as_map_get(spendable_in, (as_val*)k_off);
            as_val_destroy((as_val*)k_off);
            if (spendable_val != NULL && as_val_type(spendable_val) == AS_INTEGER) {
                int64_t spendable_height = as_integer_get(as_integer_fromval(spendable_val));
                if (spendable_height >= current_block_height) {
                    char msg[256];
                    snprintf(msg, sizeof(msg), "%s%lld", MSG_FROZEN_UNTIL, (long long)spendable_height);
                    as_hashmap* err = as_hashmap_new(2);
                    as_hashmap_set(err,
                        (as_val*)as_string_new((char*)FIELD_ERROR_CODE, false),
                        (as_val*)as_string_new((char*)ERROR_CODE_FROZEN_UNTIL, false));
                    as_hashmap_set(err,
                        (as_val*)as_string_new((char*)FIELD_MESSAGE, false),
                        (as_val*)as_string_new(strdup(msg), true));
                    as_hashmap_set(errors, (as_val*)as_integer_new(idx), (as_val*)err);

                    if (existing_spending_data) as_bytes_destroy(existing_spending_data);
                    continue;
                }
            }
        }

        // Handle already spent UTXO
        if (existing_spending_data != NULL) {
            if (utxo_bytes_equal(existing_spending_data, spending_data)) {
                // Already spent with same data - check if child tx was deleted
                if (deleted_children != NULL) {
                    // Check whether this child tx (by txid) exists in the deletedChildren map
                    as_string* child_txid = utxo_spending_data_to_txid_hex(existing_spending_data);
                    if (child_txid != NULL) {
                        as_val* deleted_val = as_map_get(deleted_children, (as_val*)child_txid);
                        if (deleted_val != NULL && as_val_type(deleted_val) != AS_NIL) {
                            // Child tx was deleted - this is an invalid spend
                            as_hashmap* err = as_hashmap_new(3);
                            as_hashmap_set(err,
                                (as_val*)as_string_new((char*)FIELD_ERROR_CODE, false),
                                (as_val*)as_string_new((char*)ERROR_CODE_INVALID_SPEND, false));
                            as_hashmap_set(err,
                                (as_val*)as_string_new((char*)FIELD_MESSAGE, false),
                                (as_val*)as_string_new((char*)MSG_INVALID_SPEND, false));
                            as_hashmap_set(err,
                                (as_val*)as_string_new((char*)FIELD_SPENDING_DATA, false),
                                (as_val*)utxo_spending_data_to_hex(existing_spending_data));
                            as_hashmap_set(errors, (as_val*)as_integer_new(idx), (as_val*)err);
                            as_string_destroy(child_txid);
                            as_bytes_destroy(existing_spending_data);
                            continue;
                        }
                        as_string_destroy(child_txid);
                    }
                }
                as_bytes_destroy(existing_spending_data);
                continue;
            } else if (utxo_is_frozen(existing_spending_data)) {
                as_hashmap* err = as_hashmap_new(2);
                as_hashmap_set(err,
                    (as_val*)as_string_new((char*)FIELD_ERROR_CODE, false),
                    (as_val*)as_string_new((char*)ERROR_CODE_FROZEN, false));
                as_hashmap_set(err,
                    (as_val*)as_string_new((char*)FIELD_MESSAGE, false),
                    (as_val*)as_string_new((char*)MSG_FROZEN, false));
                as_hashmap_set(errors, (as_val*)as_integer_new(idx), (as_val*)err);
                as_bytes_destroy(existing_spending_data);
                continue;
            } else {
                // Spent by different transaction
                as_hashmap* err = as_hashmap_new(3);
                as_hashmap_set(err,
                    (as_val*)as_string_new((char*)FIELD_ERROR_CODE, false),
                    (as_val*)as_string_new((char*)ERROR_CODE_SPENT, false));
                as_hashmap_set(err,
                    (as_val*)as_string_new((char*)FIELD_MESSAGE, false),
                    (as_val*)as_string_new((char*)MSG_SPENT, false));
                as_hashmap_set(err,
                    (as_val*)as_string_new((char*)FIELD_SPENDING_DATA, false),
                    (as_val*)utxo_spending_data_to_hex(existing_spending_data));
                as_hashmap_set(errors, (as_val*)as_integer_new(idx), (as_val*)err);
                as_bytes_destroy(existing_spending_data);
                continue;
            }
        }

        // Create new UTXO with spending data
        as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, spending_data);
        as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

        // Increment spent count
        as_val* spent_count_val = as_rec_get(rec, BIN_SPENT_UTXOS);
        int64_t spent_count = 0;
        if (spent_count_val != NULL && as_val_type(spent_count_val) == AS_INTEGER) {
            spent_count = as_integer_get(as_integer_fromval(spent_count_val));
        }
        as_rec_set(rec, BIN_SPENT_UTXOS, (as_val*)as_integer_new(spent_count + 1));

        if (existing_spending_data) as_bytes_destroy(existing_spending_data);
    }

    // Note: utxos list was modified in place, no need to call as_rec_set again
    // (calling as_rec_set with the same object would cause the map to destroy it!)
    as_rec_set(rec, BIN_UTXOS, (as_val*)utxos);

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    if (as_hashmap_size(errors) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_STATUS, false),
            (as_val*)as_string_new((char*)STATUS_ERROR, false));
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_ERRORS, false),
            (as_val*)errors);
    } else {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_STATUS, false),
            (as_val*)as_string_new((char*)STATUS_OK, false));
        as_hashmap_destroy(errors);
    }

    if (block_ids_val != NULL && as_val_type(block_ids_val) == AS_LIST) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_BLOCK_IDS, false),
            as_val_reserve(block_ids_val));
    }

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_unspend(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);
    int64_t current_block_height = get_list_int64(args, 2);
    int64_t block_height_retention = get_list_int64(args, 3);

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Only unspend if spent and not frozen
    if (as_bytes_size(utxo) == FULL_UTXO_SIZE) {
        if (utxo_is_frozen(existing_spending_data)) {
            if (existing_spending_data) as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(ERROR_CODE_FROZEN, ERR_UTXO_IS_FROZEN);
        }

        // Create unspent UTXO
        as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, NULL);
        as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);
        as_rec_set(rec, BIN_UTXOS, (as_val*)utxos);

        // Decrement spent count
        as_val* spent_count_val = as_rec_get(rec, BIN_SPENT_UTXOS);
        int64_t spent_count = 0;
        if (spent_count_val != NULL && as_val_type(spent_count_val) == AS_INTEGER) {
            spent_count = as_integer_get(as_integer_fromval(spent_count_val));
        }
        as_rec_set(rec, BIN_SPENT_UTXOS, (as_val*)as_integer_new(spent_count - 1));
    }

    if (existing_spending_data) as_bytes_destroy(existing_spending_data);

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_set_mined(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    as_bytes* block_id = get_list_bytes(args, 0);
    int64_t block_height = get_list_int64(args, 1);
    int64_t subtree_idx = get_list_int64(args, 2);
    int64_t current_block_height = get_list_int64(args, 3);
    int64_t block_height_retention = get_list_int64(args, 4);
    bool on_longest_chain = get_list_bool(args, 5);
    bool unset_mined = get_list_bool(args, 6);

    // Initialize lists if they don't exist
    as_val* block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);
    as_list* block_ids = NULL;
    if (block_ids_val == NULL || as_val_type(block_ids_val) == AS_NIL) {
        block_ids = (as_list*)as_arraylist_new(10, 10);
        as_rec_set(rec, BIN_BLOCK_IDS, (as_val*)block_ids);
    } else {
        block_ids = as_list_fromval(block_ids_val);
    }

    as_val* block_heights_val = as_rec_get(rec, BIN_BLOCK_HEIGHTS);
    as_list* block_heights = NULL;
    if (block_heights_val == NULL || as_val_type(block_heights_val) == AS_NIL) {
        block_heights = (as_list*)as_arraylist_new(10, 10);
        as_rec_set(rec, BIN_BLOCK_HEIGHTS, (as_val*)block_heights);
    } else {
        block_heights = as_list_fromval(block_heights_val);
    }

    as_val* subtree_idxs_val = as_rec_get(rec, BIN_SUBTREE_IDXS);
    as_list* subtree_idxs = NULL;
    if (subtree_idxs_val == NULL || as_val_type(subtree_idxs_val) == AS_NIL) {
        subtree_idxs = (as_list*)as_arraylist_new(10, 10);
        as_rec_set(rec, BIN_SUBTREE_IDXS, (as_val*)subtree_idxs);
    } else {
        subtree_idxs = as_list_fromval(subtree_idxs_val);
    }

    if (unset_mined) {
        // Remove blockID and corresponding entries
        uint32_t block_count = as_list_size(block_ids);
        int32_t found_idx = -1;

        for (uint32_t i = 0; i < block_count; i++) {
            as_bytes* existing_block = as_bytes_fromval(as_list_get(block_ids, i));
            if (existing_block != NULL && utxo_bytes_equal(existing_block, block_id)) {
                found_idx = (int32_t)i;
                break;
            }
        }

        if (found_idx >= 0) {
            as_list_remove(block_ids, (uint32_t)found_idx);
            as_list_remove(block_heights, (uint32_t)found_idx);
            as_list_remove(subtree_idxs, (uint32_t)found_idx);

            as_rec_set(rec, BIN_BLOCK_IDS, (as_val*)block_ids);
            as_rec_set(rec, BIN_BLOCK_HEIGHTS, (as_val*)block_heights);
            as_rec_set(rec, BIN_SUBTREE_IDXS, (as_val*)subtree_idxs);
        }
    } else {
        // Add blockID if not already exists
        uint32_t block_count = as_list_size(block_ids);
        bool block_exists = false;

        for (uint32_t i = 0; i < block_count; i++) {
            as_bytes* existing_block = as_bytes_fromval(as_list_get(block_ids, i));
            if (existing_block != NULL && utxo_bytes_equal(existing_block, block_id)) {
                block_exists = true;
                break;
            }
        }

        if (!block_exists) {
            as_list_append(block_ids, as_val_reserve((as_val*)block_id));
            as_list_append(block_heights, (as_val*)as_integer_new(block_height));
            as_list_append(subtree_idxs, (as_val*)as_integer_new(subtree_idx));

            as_rec_set(rec, BIN_BLOCK_IDS, (as_val*)block_ids);
            as_rec_set(rec, BIN_BLOCK_HEIGHTS, (as_val*)block_heights);
            as_rec_set(rec, BIN_SUBTREE_IDXS, (as_val*)subtree_idxs);
        }
    }

    // Handle unminedSince based on block count
    uint32_t has_blocks = as_list_size(block_ids);
    if (has_blocks > 0) {
        if (on_longest_chain) {
            as_rec_set(rec, BIN_UNMINED_SINCE, (as_val*)&as_nil);
        }
    } else {
        as_rec_set(rec, BIN_UNMINED_SINCE, (as_val*)as_integer_new(current_block_height));
    }

    // Clear locked flag if set
    as_val* locked_val = as_rec_get(rec, BIN_LOCKED);
    if (locked_val != NULL && as_val_type(locked_val) != AS_NIL) {
        as_rec_set(rec, BIN_LOCKED, (as_val*)as_boolean_new(false));
    }

    // Clear creating flag if set (remove from record)
    as_val* creating_val = as_rec_get(rec, BIN_CREATING);
    if (creating_val != NULL && as_val_type(creating_val) != AS_NIL) {
        as_rec_set(rec, BIN_CREATING, (as_val*)&as_nil);
    }

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    as_hashmap_set(response,
        (as_val*)as_string_new((char*)FIELD_BLOCK_IDS, false),
        as_val_reserve((as_val*)block_ids));

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_freeze(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // If UTXO has spending data, check if already frozen
    if (existing_spending_data != NULL) {
        if (utxo_is_frozen(existing_spending_data)) {
            as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(ERROR_CODE_ALREADY_FROZEN, MSG_ALREADY_FROZEN);
        } else {
            // Already spent by something else
            as_map* err = utxo_create_error_response(ERROR_CODE_SPENT, MSG_SPENT);
            as_hashmap_set((as_hashmap*)err,
                (as_val*)as_string_new((char*)FIELD_SPENDING_DATA, false),
                (as_val*)utxo_spending_data_to_hex(existing_spending_data));
            as_bytes_destroy(existing_spending_data);
            return (as_val*)err;
        }
    }

    // Check UTXO is unspent (size should be 32)
    if (as_bytes_size(utxo) != UTXO_HASH_SIZE) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
    }

    // Create frozen spending data (all bytes = 255)
    uint8_t frozen_buf[SPENDING_DATA_SIZE];
    memset(frozen_buf, FROZEN_BYTE, SPENDING_DATA_SIZE);

    as_bytes* frozen_data = as_bytes_new(SPENDING_DATA_SIZE);
    as_bytes_set(frozen_data, 0, frozen_buf, SPENDING_DATA_SIZE);

    // Create new UTXO with frozen data
    as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, frozen_data);
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);
    as_rec_set(rec, BIN_UTXOS, (as_val*)utxos);

    as_bytes_destroy(frozen_data);

    return (as_val*)utxo_create_ok_response();
}

as_val*
teranode_unfreeze(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Check that UTXO is actually frozen (size=68 with frozen pattern)
    // Unspent UTXOs (size=32) or spent UTXOs (size=68 with non-frozen data) are not frozen
    if (as_bytes_size(utxo) != FULL_UTXO_SIZE || existing_spending_data == NULL || !utxo_is_frozen(existing_spending_data)) {
        if (existing_spending_data) as_bytes_destroy(existing_spending_data);
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_NOT_FROZEN, ERR_UTXO_NOT_FROZEN);
    }

    // Create unspent UTXO (remove spending data)
    as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, NULL);
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);
    as_rec_set(rec, BIN_UTXOS, (as_val*)utxos);

    as_bytes_destroy(existing_spending_data);

    return (as_val*)utxo_create_ok_response();
}

as_val*
teranode_reassign(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);
    as_bytes* new_utxo_hash = get_list_bytes(args, 2);
    int64_t block_height = get_list_int64(args, 3);
    int64_t spendable_after = get_list_int64(args, 4);

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Check that UTXO is frozen (required for reassignment)
    // Unspent UTXOs (size=32) or spent UTXOs (size=68 with non-frozen data) are not frozen
    if (as_bytes_size(utxo) != FULL_UTXO_SIZE || existing_spending_data == NULL || !utxo_is_frozen(existing_spending_data)) {
        if (existing_spending_data) as_bytes_destroy(existing_spending_data);
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_NOT_FROZEN, ERR_UTXO_NOT_FROZEN);
    }

    // Create new UTXO with new hash (unspent)
    as_bytes* new_utxo = utxo_create_with_spending_data(new_utxo_hash, NULL);
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);
    as_rec_set(rec, BIN_UTXOS, (as_val*)utxos);

    // Initialize reassignments list if needed
    as_val* reassignments_val = as_rec_get(rec, BIN_REASSIGNMENTS);
    as_list* reassignments = NULL;
    if (reassignments_val == NULL || as_val_type(reassignments_val) == AS_NIL) {
        reassignments = (as_list*)as_arraylist_new(10, 10);
        as_rec_set(rec, BIN_REASSIGNMENTS, (as_val*)reassignments);
    } else {
        reassignments = as_list_fromval(reassignments_val);
    }

    // Initialize utxoSpendableIn map if needed
    as_val* spendable_in_val = as_rec_get(rec, BIN_UTXO_SPENDABLE_IN);
    as_map* spendable_in = NULL;
    if (spendable_in_val == NULL || as_val_type(spendable_in_val) == AS_NIL) {
        spendable_in = (as_map*)as_hashmap_new(10);
        as_rec_set(rec, BIN_UTXO_SPENDABLE_IN, (as_val*)spendable_in);
    } else {
        spendable_in = as_map_fromval(spendable_in_val);
    }

    // Record reassignment details
    as_hashmap* reassignment_entry = as_hashmap_new(4);
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"offset", false),
        (as_val*)as_integer_new(offset));
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"utxoHash", false),
        as_val_reserve((as_val*)utxo_hash));
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"newUtxoHash", false),
        as_val_reserve((as_val*)new_utxo_hash));
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"blockHeight", false),
        (as_val*)as_integer_new(block_height));

    as_list_append(reassignments, (as_val*)reassignment_entry);
    as_rec_set(rec, BIN_REASSIGNMENTS, (as_val*)reassignments);

    // Set spendable height
    as_map_set(spendable_in,
        (as_val*)as_integer_new(offset),
        (as_val*)as_integer_new(block_height + spendable_after));
    as_rec_set(rec, BIN_UTXO_SPENDABLE_IN, (as_val*)spendable_in);

    // Increment recordUtxos to prevent deletion
    as_val* record_utxos_val = as_rec_get(rec, BIN_RECORD_UTXOS);
    int64_t record_utxos = 0;
    if (record_utxos_val != NULL && as_val_type(record_utxos_val) == AS_INTEGER) {
        record_utxos = as_integer_get(as_integer_fromval(record_utxos_val));
    }
    as_rec_set(rec, BIN_RECORD_UTXOS, (as_val*)as_integer_new(record_utxos + 1));

    as_bytes_destroy(existing_spending_data);

    return (as_val*)utxo_create_ok_response();
}

as_val*
teranode_set_conflicting(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    bool set_value = get_list_bool(args, 0);
    int64_t current_block_height = get_list_int64(args, 1);
    int64_t block_height_retention = get_list_int64(args, 2);

    // Set conflicting flag
    as_rec_set(rec, BIN_CONFLICTING, (as_val*)as_boolean_new(set_value));

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_preserve_until(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t block_height = get_list_int64(args, 0);

    // Remove deleteAtHeight if it exists
    as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)&as_nil);

    // Set preserveUntil
    as_rec_set(rec, BIN_PRESERVE_UNTIL, (as_val*)as_integer_new(block_height));

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();

    // Check if we need to signal external file handling
    as_val* external_val = as_rec_get(rec, BIN_EXTERNAL);
    if (external_val != NULL && as_val_type(external_val) != AS_NIL) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)SIGNAL_PRESERVE, false));
    }

    return (as_val*)response;
}

as_val*
teranode_set_locked(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    bool set_value = get_list_bool(args, 0);

    // Get totalExtraRecs (default to 0)
    as_val* total_extra_recs_val = as_rec_get(rec, BIN_TOTAL_EXTRA_RECS);
    int64_t total_extra_recs = 0;
    if (total_extra_recs_val != NULL && as_val_type(total_extra_recs_val) == AS_INTEGER) {
        total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));
    }

    // Set locked flag
    as_rec_set(rec, BIN_LOCKED, (as_val*)as_boolean_new(set_value));

    // Remove deleteAtHeight when locking
    if (set_value) {
        as_val* existing_dah_val = as_rec_get(rec, BIN_DELETE_AT_HEIGHT);
        if (existing_dah_val != NULL && as_val_type(existing_dah_val) != AS_NIL) {
            as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)&as_nil);
        }
    }

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    as_hashmap_set(response,
        (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
        (as_val*)as_integer_new(total_extra_recs));

    return (as_val*)response;
}

as_val*
teranode_increment_spent_extra_recs(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t inc = get_list_int64(args, 0);
    int64_t current_block_height = get_list_int64(args, 1);
    int64_t block_height_retention = get_list_int64(args, 2);

    // Check totalExtraRecs exists
    as_val* total_extra_recs_val = as_rec_get(rec, BIN_TOTAL_EXTRA_RECS);
    if (total_extra_recs_val == NULL || as_val_type(total_extra_recs_val) == AS_NIL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, ERR_TOTAL_EXTRA_RECS);
    }
    int64_t total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));

    // Get spentExtraRecs (default to 0)
    as_val* spent_extra_recs_val = as_rec_get(rec, BIN_SPENT_EXTRA_RECS);
    int64_t spent_extra_recs = 0;
    if (spent_extra_recs_val != NULL && as_val_type(spent_extra_recs_val) == AS_INTEGER) {
        spent_extra_recs = as_integer_get(as_integer_fromval(spent_extra_recs_val));
    }

    // Increment
    spent_extra_recs += inc;

    // Validate bounds
    if (spent_extra_recs < 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, ERR_SPENT_EXTRA_RECS_NEGATIVE);
    }

    if (spent_extra_recs > total_extra_recs) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, ERR_SPENT_EXTRA_RECS_EXCEED);
    }

    // Update bin
    as_rec_set(rec, BIN_SPENT_EXTRA_RECS, (as_val*)as_integer_new(spent_extra_recs));

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_set_delete_at_height(as_rec* rec, as_list* args)
{
    int64_t current_block_height = get_list_int64(args, 0);
    int64_t block_height_retention = get_list_int64(args, 1);

    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}
