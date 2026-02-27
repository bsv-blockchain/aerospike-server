/*
 * mod_teranode_utxo.h
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
 * UTXO function declarations and constants.
 */

#pragma once

#include <aerospike/as_rec.h>
#include <aerospike/as_list.h>
#include <aerospike/as_map.h>
#include <aerospike/as_val.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_string.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_aerospike.h>
#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

//==========================================================
// UTXO Constants (from teranode.lua).
//

#define UTXO_HASH_SIZE          32
#define SPENDING_DATA_SIZE      36
#define FULL_UTXO_SIZE          (UTXO_HASH_SIZE + SPENDING_DATA_SIZE)  // 68
#define FROZEN_BYTE             255

//==========================================================
// Bin Name Constants.
//

#define BIN_BLOCK_HEIGHTS       "blockHeights"
#define BIN_BLOCK_IDS           "blockIDs"
#define BIN_CONFLICTING         "conflicting"
#define BIN_DELETE_AT_HEIGHT    "deleteAtHeight"
#define BIN_EXTERNAL            "external"
#define BIN_UNMINED_SINCE       "unminedSince"
#define BIN_PRESERVE_UNTIL      "preserveUntil"
#define BIN_REASSIGNMENTS       "reassignments"
#define BIN_RECORD_UTXOS        "recordUtxos"
#define BIN_SPENDING_HEIGHT     "spendingHeight"
#define BIN_SPENT_EXTRA_RECS    "spentExtraRecs"
#define BIN_SPENT_UTXOS         "spentUtxos"
#define BIN_SUBTREE_IDXS        "subtreeIdxs"
#define BIN_TOTAL_EXTRA_RECS    "totalExtraRecs"
#define BIN_LOCKED              "locked"
#define BIN_CREATING            "creating"
#define BIN_UTXOS               "utxos"
#define BIN_UTXO_SPENDABLE_IN   "utxoSpendableIn"
#define BIN_LAST_SPENT_STATE    "lastSpentState"
#define BIN_DELETED_CHILDREN    "deletedChildren"

//==========================================================
// Status Constants.
//

#define STATUS_OK               "OK"
#define STATUS_ERROR            "ERROR"

//==========================================================
// Error Code Constants.
//

#define ERROR_CODE_TX_NOT_FOUND         "TX_NOT_FOUND"
#define ERROR_CODE_CONFLICTING          "CONFLICTING"
#define ERROR_CODE_LOCKED               "LOCKED"
#define ERROR_CODE_CREATING             "CREATING"
#define ERROR_CODE_FROZEN               "FROZEN"
#define ERROR_CODE_ALREADY_FROZEN       "ALREADY_FROZEN"
#define ERROR_CODE_FROZEN_UNTIL         "FROZEN_UNTIL"
#define ERROR_CODE_COINBASE_IMMATURE    "COINBASE_IMMATURE"
#define ERROR_CODE_SPENT                "SPENT"
#define ERROR_CODE_INVALID_SPEND        "INVALID_SPEND"
#define ERROR_CODE_UTXOS_NOT_FOUND      "UTXOS_NOT_FOUND"
#define ERROR_CODE_UTXO_NOT_FOUND       "UTXO_NOT_FOUND"
#define ERROR_CODE_UTXO_INVALID_SIZE    "UTXO_INVALID_SIZE"
#define ERROR_CODE_UTXO_HASH_MISMATCH   "UTXO_HASH_MISMATCH"
#define ERROR_CODE_UTXO_NOT_FROZEN      "UTXO_NOT_FROZEN"
#define ERROR_CODE_INVALID_PARAMETER    "INVALID_PARAMETER"
#define ERROR_CODE_UPDATE_FAILED        "UPDATE_FAILED"

//==========================================================
// Message Constants.
//

#define MSG_CONFLICTING             "TX is conflicting"
#define MSG_LOCKED                  "TX is locked and cannot be spent"
#define MSG_CREATING                "TX is being created and cannot be spent yet"
#define MSG_FROZEN                  "UTXO is frozen"
#define MSG_ALREADY_FROZEN          "UTXO is already frozen"
#define MSG_FROZEN_UNTIL            "UTXO is not spendable until block "
#define MSG_COINBASE_IMMATURE       "Coinbase UTXO can only be spent when it matures"
#define MSG_SPENT                   "Already spent by "
#define MSG_INVALID_SPEND           "Invalid spend"

//==========================================================
// Signal Constants.
//

#define SIGNAL_ALL_SPENT            "ALLSPENT"
#define SIGNAL_NOT_ALL_SPENT        "NOTALLSPENT"
#define SIGNAL_DELETE_AT_HEIGHT_SET "DAHSET"
#define SIGNAL_DELETE_AT_HEIGHT_UNSET "DAHUNSET"
#define SIGNAL_PRESERVE             "PRESERVE"

//==========================================================
// Error Message Constants.
//

#define ERR_TX_NOT_FOUND                "TX not found"
#define ERR_UTXOS_NOT_FOUND             "UTXOs list not found"
#define ERR_UTXO_NOT_FOUND              "UTXO not found for offset "
#define ERR_UTXO_INVALID_SIZE           "UTXO has an invalid size"
#define ERR_UTXO_HASH_MISMATCH          "Output utxohash mismatch"
#define ERR_UTXO_NOT_FROZEN             "UTXO is not frozen"
#define ERR_UTXO_IS_FROZEN              "UTXO is frozen"
#define ERR_SPENT_EXTRA_RECS_NEGATIVE   "spentExtraRecs cannot be negative"
#define ERR_SPENT_EXTRA_RECS_EXCEED     "spentExtraRecs cannot be greater than totalExtraRecs"
#define ERR_TOTAL_EXTRA_RECS            "totalExtraRecs not found in record. Possible non-master record?"
#define ERR_UPDATE_FAILED               "Failed to commit record changes"

//==========================================================
// Response Field Names.
//

#define FIELD_STATUS            "status"
#define FIELD_ERROR_CODE        "errorCode"
#define FIELD_MESSAGE           "message"
#define FIELD_SIGNAL            "signal"
#define FIELD_BLOCK_IDS         "blockIDs"
#define FIELD_ERRORS            "errors"
#define FIELD_CHILD_COUNT       "childCount"
#define FIELD_SPENDING_DATA     "spendingData"

//==========================================================
// UTXO Function Declarations.
//
// Each function receives:
//   - rec: The record to operate on
//   - args: Arguments as as_list
//   - as: The aerospike context for calling rec_update
// Returns:
//   - as_val* (typically as_map*) with result
//

/**
 * Mark a single UTXO as spent.
 *
 * Args (as_list):
 *   [0] offset (int64)           - offset in utxos list
 *   [1] utxoHash (bytes[32])     - hash of UTXO
 *   [2] spendingData (bytes[36]) - spending transaction data
 *   [3] ignoreConflicting (bool) - ignore conflicting flag
 *   [4] ignoreLocked (bool)      - ignore locked flag
 *   [5] currentBlockHeight (int64)
 *   [6] blockHeightRetention (int64)
 */
as_val* teranode_spend(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Mark multiple UTXOs as spent in a single operation.
 *
 * Args (as_list):
 *   [0] spends (list of maps)    - each map has: offset, utxoHash, spendingData, idx
 *   [1] ignoreConflicting (bool)
 *   [2] ignoreLocked (bool)
 *   [3] currentBlockHeight (int64)
 *   [4] blockHeightRetention (int64)
 */
as_val* teranode_spend_multi(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Reverse a spend operation (mark UTXO as unspent).
 *
 * Args (as_list):
 *   [0] offset (int64)
 *   [1] utxoHash (bytes[32])
 *   [2] currentBlockHeight (int64)
 *   [3] blockHeightRetention (int64)
 */
as_val* teranode_unspend(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Track which block(s) a transaction is mined in.
 *
 * Args (as_list):
 *   [0] blockID (int64)
 *   [1] blockHeight (int64)
 *   [2] subtreeIdx (int64)
 *   [3] currentBlockHeight (int64)
 *   [4] blockHeightRetention (int64)
 *   [5] onLongestChain (bool)
 *   [6] unsetMined (bool)        - if true, removes block instead of adding
 */
as_val* teranode_set_mined(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Freeze a UTXO (prevent spending).
 *
 * Args (as_list):
 *   [0] offset (int64)
 *   [1] utxoHash (bytes[32])
 */
as_val* teranode_freeze(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Unfreeze a frozen UTXO.
 *
 * Args (as_list):
 *   [0] offset (int64)
 *   [1] utxoHash (bytes[32])
 */
as_val* teranode_unfreeze(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Reassign a UTXO hash (for frozen UTXOs).
 *
 * Args (as_list):
 *   [0] offset (int64)
 *   [1] utxoHash (bytes[32])
 *   [2] newUtxoHash (bytes[32])
 *   [3] blockHeight (int64)
 *   [4] spendableAfter (int64)
 */
as_val* teranode_reassign(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Mark a transaction as conflicting.
 *
 * Args (as_list):
 *   [0] setValue (bool)          - true to set, false to clear
 *   [1] currentBlockHeight (int64)
 *   [2] blockHeightRetention (int64)
 */
as_val* teranode_set_conflicting(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Preserve a transaction until a specific block height (prevent deletion).
 *
 * Args (as_list):
 *   [0] blockHeight (int64)
 */
as_val* teranode_preserve_until(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Lock/unlock a transaction from being spent.
 *
 * Args (as_list):
 *   [0] setValue (bool)          - true to lock, false to unlock
 */
as_val* teranode_set_locked(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Increment/decrement the spent extra records counter.
 *
 * Args (as_list):
 *   [0] inc (int64)              - amount to increment (can be negative)
 *   [1] currentBlockHeight (int64)
 *   [2] blockHeightRetention (int64)
 */
as_val* teranode_increment_spent_extra_recs(as_rec* rec, as_list* args, as_aerospike* as);

/**
 * Internal helper: Set the deleteAtHeight for a record.
 * Called by other functions, not typically called directly.
 *
 * Args (as_list):
 *   [0] currentBlockHeight (int64)
 *   [1] blockHeightRetention (int64)
 *
 * Returns signal string and optional childCount.
 */
as_val* teranode_set_delete_at_height(as_rec* rec, as_list* args, as_aerospike* as);

//==========================================================
// Internal Helper Function Declarations.
//

/**
 * Compare two byte arrays for equality.
 */
bool utxo_bytes_equal(as_bytes* a, as_bytes* b);

/**
 * Check if spending data indicates a frozen UTXO (all bytes = 255).
 * Takes a raw pointer to SPENDING_DATA_SIZE bytes.
 */
bool utxo_is_frozen(const uint8_t* spending_data);

/**
 * Create a new UTXO bytes object with optional spending data.
 * If spending_data is NULL, creates 32-byte UTXO (unspent).
 * If spending_data is provided, creates 68-byte UTXO (spent).
 */
as_bytes* utxo_create_with_spending_data(as_bytes* utxo_hash, as_bytes* spending_data);

/**
 * Get UTXO and spending data from utxos list at offset.
 * Validates the hash matches expected_hash.
 *
 * Returns:
 *   0 on success, sets *out_utxo and *out_spending_data
 *   Non-zero on error, sets *out_error_response
 *
 * Note: out_spending_data points into the existing UTXO bytes (no allocation).
 *       The pointer is valid as long as the UTXO list element is not replaced.
 */
int utxo_get_and_validate(
    as_list* utxos,
    int64_t offset,
    as_bytes* expected_hash,
    as_bytes** out_utxo,
    const uint8_t** out_spending_data,
    as_map** out_error_response
);

/**
 * Convert spending data bytes to hex string for error messages.
 * Format: reversed txid (32 bytes) + vin (4 bytes little-endian)
 * Takes a raw pointer to SPENDING_DATA_SIZE bytes.
 */
as_string* utxo_spending_data_to_hex(const uint8_t* spending_data);

/**
 * Create an error response map.
 */
as_map* utxo_create_error_response(const char* error_code, const char* message);

/**
 * Create a success response map.
 */
as_map* utxo_create_ok_response(void);

/**
 * Internal implementation of setDeleteAtHeight logic.
 * Called by other functions to manage record deletion timing.
 *
 * Returns signal string (or empty string) and sets *out_child_count if applicable.
 */
const char* utxo_set_delete_at_height_impl(
    as_rec* rec,
    int64_t current_block_height,
    int64_t block_height_retention,
    int64_t* out_child_count
);

#ifdef __cplusplus
}
#endif
