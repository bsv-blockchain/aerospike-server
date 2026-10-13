/*
 * test_state_management.c
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
 * Tests for state management functions (setMined, setConflicting, setLocked, etc.).
 */

#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_arraylist.h>
#include "test_framework.h"
#include "mock_record.h"

//==========================================================
// setMined() tests.
//

TEST(setMined_add_block)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    int64_t block_id = 12345;

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, block_id);  // blockID as integer
    as_arraylist_append_int64(args, 500);   // blockHeight
    as_arraylist_append_int64(args, 1);     // subtreeIdx
    as_arraylist_append_int64(args, 1000);  // currentBlockHeight
    as_arraylist_append_int64(args, 100);   // blockHeightRetention
    as_arraylist_append(args, (as_val*)as_boolean_new(true));   // onLongestChain
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // unsetMined

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify blockIDs list created and has 1 entry
    as_val* block_ids_val = as_rec_get(rec, "blockIDs");
    ASSERT_NOT_NULL(block_ids_val);
    as_list* block_ids = as_list_fromval(block_ids_val);
    ASSERT_EQ(as_list_size(block_ids), 1);

    // Verify the stored block ID matches
    as_integer* stored_id = as_integer_fromval(as_list_get(block_ids, 0));
    ASSERT_EQ(as_integer_get(stored_id), block_id);

    // Verify block heights list
    as_val* heights_val = as_rec_get(rec, "blockHeights");
    ASSERT_NOT_NULL(heights_val);

    // Verify subtree indexes list
    as_val* subtree_val = as_rec_get(rec, "subtreeIdxs");
    ASSERT_NOT_NULL(subtree_val);

    // Verify unminedSince is nil (on longest chain)
    as_val* unmined_val = as_rec_get(rec, "unminedSince");
    ASSERT_TRUE(unmined_val == NULL || as_val_type(unmined_val) == AS_NIL);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_remove_block)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    int64_t block_id = 54321;

    // Add a block first
    as_arraylist* args1 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args1, block_id);  // blockID as integer
    as_arraylist_append_int64(args1, 500);
    as_arraylist_append_int64(args1, 1);
    as_arraylist_append_int64(args1, 1000);
    as_arraylist_append_int64(args1, 100);
    as_arraylist_append(args1, (as_val*)as_boolean_new(true));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));  // unsetMined = false

    as_val* result1 = teranode_set_mined(rec, (as_list*)args1, as_ctx);
    as_val_destroy(result1);

    // Now remove it
    as_arraylist* args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args2, block_id);  // blockID as integer
    as_arraylist_append_int64(args2, 500);
    as_arraylist_append_int64(args2, 1);
    as_arraylist_append_int64(args2, 1000);
    as_arraylist_append_int64(args2, 100);
    as_arraylist_append(args2, (as_val*)as_boolean_new(true));
    as_arraylist_append(args2, (as_val*)as_boolean_new(true));   // unsetMined = true

    as_val* result2 = teranode_set_mined(rec, (as_list*)args2, as_ctx);
    as_map* result_map = as_map_fromval(result2);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify blockIDs list is now empty
    as_val* block_ids_val = as_rec_get(rec, "blockIDs");
    as_list* block_ids = as_list_fromval(block_ids_val);
    ASSERT_EQ(as_list_size(block_ids), 0);

    as_arraylist_destroy(args1);
    as_arraylist_destroy(args2);
    as_val_destroy(result2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_clears_locked)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "locked", (as_val*)as_boolean_new(true));

    int64_t block_id = 99999;

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, block_id);  // blockID as integer
    as_arraylist_append_int64(args, 500);
    as_arraylist_append_int64(args, 1);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    // Verify locked is now false
    as_val* locked_val = as_rec_get(rec, "locked");
    ASSERT_FALSE(as_boolean_get(as_boolean_fromval(locked_val)));

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_clears_creating)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "creating", (as_val*)as_boolean_new(true));

    int64_t block_id = 88888;

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, block_id);  // blockID as integer
    as_arraylist_append_int64(args, 500);
    as_arraylist_append_int64(args, 1);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    // Verify creating is nil
    as_val* creating_val = as_rec_get(rec, "creating");
    ASSERT_TRUE(creating_val == NULL || as_val_type(creating_val) == AS_NIL);

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Helper: unsetMined of the only block on a record whose blockIDs is [block_id],
// passing current_block_height as the stamp height.
static void unset_only_block(as_rec* rec, as_aerospike* as_ctx,
        int64_t block_id, int64_t current_block_height)
{
    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, block_id);
    as_arraylist_append_int64(args, 500);
    as_arraylist_append_int64(args, 1);
    as_arraylist_append_int64(args, current_block_height);
    as_arraylist_append_int64(args, 100);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // onLongestChain
    as_arraylist_append(args, (as_val*)as_boolean_new(true));   // unsetMined

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);
    as_arraylist_destroy(args);
}

static void set_single_block_bins(as_rec* rec, int64_t block_id)
{
    as_arraylist* ids = as_arraylist_new(1, 0);
    as_arraylist_append_int64(ids, block_id);
    as_rec_set(rec, "blockIDs", (as_val*)ids);
    as_arraylist* heights = as_arraylist_new(1, 0);
    as_arraylist_append_int64(heights, 500);
    as_rec_set(rec, "blockHeights", (as_val*)heights);
    as_arraylist* idxs = as_arraylist_new(1, 0);
    as_arraylist_append_int64(idxs, 1);
    as_rec_set(rec, "subtreeIdxs", (as_val*)idxs);
}

// Lua parity: removing the last block keeps an existing unminedSince.
TEST(setMined_unset_last_block_keeps_existing_unmined_since)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    set_single_block_bins(rec, 54321);
    as_rec_set(rec, "unminedSince", (as_val*)as_integer_new(700));

    unset_only_block(rec, as_ctx, 54321, 1000);

    as_val* v = as_rec_get(rec, "unminedSince");
    ASSERT_TRUE(v != NULL && as_val_type(v) == AS_INTEGER);
    ASSERT_EQ(as_integer_get(as_integer_fromval(v)), 700);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Lua parity: removing the last block stamps the passed height when absent.
TEST(setMined_unset_last_block_stamps_unmined_since_when_absent)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    set_single_block_bins(rec, 54321);

    unset_only_block(rec, as_ctx, 54321, 900);

    as_val* v = as_rec_get(rec, "unminedSince");
    ASSERT_TRUE(v != NULL && as_val_type(v) == AS_INTEGER);
    ASSERT_EQ(as_integer_get(as_integer_fromval(v)), 900);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Lua parity (teranode#1186): setMined stamps deleteAtHeight from the mined
// block's height, not from the caller's (possibly lagging/backdated)
// currentBlockHeight.
TEST(setMined_dah_stamped_from_block_height)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 2);
    as_rec_set(rec, "spentUtxos", (as_val*)as_integer_new(2));
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(0));

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 54321);   // blockID
    as_arraylist_append_int64(args, 500);     // blockHeight
    as_arraylist_append_int64(args, 1);       // subtreeIdx
    as_arraylist_append_int64(args, 450);     // currentBlockHeight (lagging)
    as_arraylist_append_int64(args, 100);     // retention
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);
    as_arraylist_destroy(args);

    as_val* dah = as_rec_get(rec, "deleteAtHeight");
    ASSERT_TRUE(dah != NULL && as_val_type(dah) == AS_INTEGER);
    ASSERT_EQ(as_integer_get(as_integer_fromval(dah)), 600);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Calls teranode_set_mined with the 7 positional args and returns the
// response (caller destroys).
static as_val* sm_call(as_rec* rec, as_aerospike* as_ctx, int64_t block_id,
        int64_t block_height, int64_t current_height, int64_t retention,
        bool on_longest_chain, bool unset_mined)
{
    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, block_id);
    as_arraylist_append_int64(args, block_height);
    as_arraylist_append_int64(args, 1);
    as_arraylist_append_int64(args, current_height);
    as_arraylist_append_int64(args, retention);
    as_arraylist_append(args, (as_val*)as_boolean_new(on_longest_chain));
    as_arraylist_append(args, (as_val*)as_boolean_new(unset_mined));
    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_arraylist_destroy(args);
    return result;
}

// Fully spent master record (2 of 2 UTXOs spent, no extra records).
static void sm_init_all_spent(as_rec* rec)
{
    mock_rec_init_utxos(rec, 2);
    as_rec_set(rec, "spentUtxos", (as_val*)as_integer_new(2));
    as_rec_set(rec, "recordUtxos", (as_val*)as_integer_new(2));
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(0));
}

static bool sm_int_bin(as_rec* rec, const char* name, int64_t* out)
{
    as_val* v = as_rec_get(rec, name);
    if (v == NULL || as_val_type(v) != AS_INTEGER) return false;
    *out = as_integer_get(as_integer_fromval(v));
    return true;
}

// Lua parity (teranode#1037): a mine always returns childCount =
// totalExtraRecs when > 0, even with no DAH signal; an unset does not.
TEST(setMined_mine_returns_child_count_without_signal)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 2);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(3));

    as_val* r = sm_call(rec, as_ctx, 54321, 500, 450, 100, true, false);
    as_map* m = as_map_fromval(r);
    as_val* cc = as_map_get(m, (as_val*)as_string_new((char*)"childCount", false));
    ASSERT_TRUE(cc != NULL && as_val_type(cc) == AS_INTEGER);
    ASSERT_EQ(as_integer_get(as_integer_fromval(cc)), 3);
    as_val_destroy(r);

    r = sm_call(rec, as_ctx, 54321, 500, 450, 100, true, true);
    m = as_map_fromval(r);
    cc = as_map_get(m, (as_val*)as_string_new((char*)"childCount", false));
    ASSERT_TRUE(cc == NULL);
    as_val_destroy(r);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Lua parity: on-longest-chain comes from the record (unminedSince absent),
// not from the onLongestChain arg. Adding a side-chain block to a record
// already on the longest chain keeps the DAH.
TEST(setMined_side_chain_block_keeps_dah_when_unmined_since_nil)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    sm_init_all_spent(rec);
    set_single_block_bins(rec, 111);
    as_rec_set(rec, "deleteAtHeight", (as_val*)as_integer_new(600));

    as_val* r = sm_call(rec, as_ctx, 222, 500, 450, 100, false, false);
    as_val_destroy(r);

    int64_t dah = 0;
    ASSERT_TRUE(sm_int_bin(rec, "deleteAtHeight", &dah));
    ASSERT_EQ(dah, 600);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_stale_higher_dah_unchanged)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    sm_init_all_spent(rec);
    as_rec_set(rec, "deleteAtHeight", (as_val*)as_integer_new(900));

    as_val* r = sm_call(rec, as_ctx, 54321, 500, 450, 100, true, false);
    as_val_destroy(r);

    int64_t dah = 0;
    ASSERT_TRUE(sm_int_bin(rec, "deleteAtHeight", &dah));
    ASSERT_EQ(dah, 900);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_unset_last_block_clears_existing_dah)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    sm_init_all_spent(rec);
    set_single_block_bins(rec, 54321);
    as_rec_set(rec, "deleteAtHeight", (as_val*)as_integer_new(600));

    as_val* r = sm_call(rec, as_ctx, 54321, 500, 450, 100, false, true);
    as_val_destroy(r);

    int64_t dah = 0;
    ASSERT_FALSE(sm_int_bin(rec, "deleteAtHeight", &dah));

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_retention_zero_sets_no_dah)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    sm_init_all_spent(rec);

    as_val* r = sm_call(rec, as_ctx, 54321, 500, 450, 0, true, false);
    as_val_destroy(r);

    int64_t dah = 0;
    ASSERT_FALSE(sm_int_bin(rec, "deleteAtHeight", &dah));

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_preserve_until_sets_no_dah)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    sm_init_all_spent(rec);
    as_rec_set(rec, "preserveUntil", (as_val*)as_integer_new(5000));

    as_val* r = sm_call(rec, as_ctx, 54321, 500, 450, 100, true, false);
    as_val_destroy(r);

    int64_t dah = 0;
    ASSERT_FALSE(sm_int_bin(rec, "deleteAtHeight", &dah));

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setMined_conflicting_dah_uses_block_height)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 2);
    as_rec_set(rec, "conflicting", (as_val*)as_boolean_new(true));

    as_val* r = sm_call(rec, as_ctx, 54321, 500, 450, 100, true, false);
    as_val_destroy(r);

    int64_t dah = 0;
    ASSERT_TRUE(sm_int_bin(rec, "deleteAtHeight", &dah));
    ASSERT_EQ(dah, 600);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Mining on the longest chain clears unminedSince and then the DAH
// evaluation sees the record as on-chain (Lua reads the bin after the update).
TEST(setMined_mine_on_longest_chain_clears_unmined_since_and_sets_dah)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    sm_init_all_spent(rec);
    as_rec_set(rec, "unminedSince", (as_val*)as_integer_new(300));

    as_val* r = sm_call(rec, as_ctx, 54321, 500, 450, 100, true, false);
    as_val_destroy(r);

    int64_t v = 0;
    ASSERT_FALSE(sm_int_bin(rec, "unminedSince", &v));
    ASSERT_TRUE(sm_int_bin(rec, "deleteAtHeight", &v));
    ASSERT_EQ(v, 600);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// setConflicting() tests.
//

TEST(setConflicting_set_true)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));   // setValue = true
    as_arraylist_append_int64(args, 1000);  // currentBlockHeight
    as_arraylist_append_int64(args, 100);   // blockHeightRetention

    as_val* result = teranode_set_conflicting(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify conflicting flag is set
    as_val* conflicting_val = as_rec_get(rec, "conflicting");
    ASSERT_TRUE(as_boolean_get(as_boolean_fromval(conflicting_val)));

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setConflicting_set_false)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "conflicting", (as_val*)as_boolean_new(true));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // setValue = false
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_set_conflicting(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    // Verify conflicting flag is false
    as_val* conflicting_val = as_rec_get(rec, "conflicting");
    ASSERT_FALSE(as_boolean_get(as_boolean_fromval(conflicting_val)));

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// setLocked() tests.
//

TEST(setLocked_lock)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));

    as_val* result = teranode_set_locked(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify locked flag is set
    as_val* locked_val = as_rec_get(rec, "locked");
    ASSERT_TRUE(as_boolean_get(as_boolean_fromval(locked_val)));

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setLocked_clears_delete_at_height)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "deleteAtHeight", (as_val*)as_integer_new(5000));

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));  // Lock it

    as_val* result = teranode_set_locked(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    // Verify deleteAtHeight is cleared
    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_TRUE(dah_val == NULL || as_val_type(dah_val) == AS_NIL);

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// preserveUntil() tests.
//

TEST(preserveUntil_success)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "deleteAtHeight", (as_val*)as_integer_new(5000));

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append_int64(args, 10000);  // preserveUntil = 10000

    as_val* result = teranode_preserve_until(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify preserveUntil is set
    as_val* preserve_val = as_rec_get(rec, "preserveUntil");
    ASSERT_EQ(as_integer_get(as_integer_fromval(preserve_val)), 10000);

    // Verify deleteAtHeight is cleared
    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_TRUE(dah_val == NULL || as_val_type(dah_val) == AS_NIL);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(preserveUntil_with_external)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "external", (as_val*)as_boolean_new(true));

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append_int64(args, 10000);

    as_val* result = teranode_preserve_until(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    // Should have PRESERVE signal
    as_val* signal = as_map_get(result_map, (as_val*)as_string_new((char*)"signal", false));
    ASSERT_NOT_NULL(signal);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(signal)), "PRESERVE");

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// incrementSpentExtraRecs() tests.
//

TEST(incrementSpentExtraRecs_increment)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(10));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(3));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, 2);     // increment by 2
    as_arraylist_append_int64(args, 1000);  // currentBlockHeight
    as_arraylist_append_int64(args, 100);   // blockHeightRetention

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify spentExtraRecs is now 5
    as_val* spent_val = as_rec_get(rec, "spentExtraRecs");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 5);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(incrementSpentExtraRecs_decrement)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(10));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(5));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, -2);    // decrement by 2
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify spentExtraRecs is now 3
    as_val* spent_val = as_rec_get(rec, "spentExtraRecs");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 3);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(incrementSpentExtraRecs_no_total)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    // Don't set totalExtraRecs

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, 2);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_INVALID_PARAMETER);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(incrementSpentExtraRecs_negative_result)
{
    // Clamps negative results to 0 instead of erroring (matches Lua behavior).
    // Handles counter drift from interrupted rollbacks.
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(10));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(2));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, -5);    // Would result in -3, clamped to 0
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify spentExtraRecs was clamped to 0
    as_val* spent_val = as_rec_get(rec, "spentExtraRecs");
    ASSERT_NOT_NULL(spent_val);
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 0);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(incrementSpentExtraRecs_exceeds_total)
{
    // Clamps values exceeding totalExtraRecs instead of erroring (matches Lua behavior).
    // Handles counter drift from interrupted rollbacks.
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(10));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(8));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, 5);     // Would result in 13 > 10, clamped to 10
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify spentExtraRecs was clamped to totalExtraRecs (10)
    as_val* spent_val = as_rec_get(rec, "spentExtraRecs");
    ASSERT_NOT_NULL(spent_val);
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 10);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// setDeleteAtHeight() tests.
//

TEST(setDeleteAtHeight_all_spent_master)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    // Set up as master record with all UTXOs spent
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(0));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(0));
    as_rec_set(rec, "spentUtxos", (as_val*)as_integer_new(5));
    as_rec_set(rec, "recordUtxos", (as_val*)as_integer_new(5));

    // Add at least one blockID
    as_arraylist* block_ids = as_arraylist_new(1, 0);
    int64_t block_id = 77777;
    as_arraylist_append_int64(block_ids, block_id);
    as_rec_set(rec, "blockIDs", (as_val*)block_ids);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 1000);  // currentBlockHeight
    as_arraylist_append_int64(args, 100);   // blockHeightRetention

    as_val* result = teranode_set_delete_at_height(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify deleteAtHeight is set
    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_EQ(as_integer_get(as_integer_fromval(dah_val)), 1100);  // 1000 + 100

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setDeleteAtHeight_preserve_blocks)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "preserveUntil", (as_val*)as_integer_new(5000));

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_set_delete_at_height(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    // Verify deleteAtHeight is NOT set (preserveUntil blocks it)
    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_TRUE(dah_val == NULL || as_val_type(dah_val) == AS_NIL);

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setDeleteAtHeight_child_record_signal)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    // Child record (no totalExtraRecs)
    as_rec_set(rec, "spentUtxos", (as_val*)as_integer_new(5));
    as_rec_set(rec, "recordUtxos", (as_val*)as_integer_new(5));

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_set_delete_at_height(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    // Should signal ALLSPENT (transition from NOTALLSPENT)
    as_val* signal = as_map_get(result_map, (as_val*)as_string_new((char*)"signal", false));
    ASSERT_NOT_NULL(signal);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(signal)), "ALLSPENT");

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Record existence tests.
//

TEST(setMined_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    int64_t block_id = 66666;

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, block_id);  // blockID as integer
    as_arraylist_append_int64(args, 500);
    as_arraylist_append_int64(args, 1);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setConflicting_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_set_conflicting(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(setLocked_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));

    as_val* result = teranode_set_locked(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(preserveUntil_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append_int64(args, 10000);

    as_val* result = teranode_preserve_until(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(incrementSpentExtraRecs_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, 2);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

void run_state_management_tests(void)
{
    printf("\n=== State Management Tests ===\n");

    RUN_TEST(setMined_add_block);
    RUN_TEST(setMined_remove_block);
    RUN_TEST(setMined_clears_locked);
    RUN_TEST(setMined_clears_creating);
    RUN_TEST(setMined_unset_last_block_keeps_existing_unmined_since);
    RUN_TEST(setMined_unset_last_block_stamps_unmined_since_when_absent);
    RUN_TEST(setMined_dah_stamped_from_block_height);
    RUN_TEST(setMined_mine_returns_child_count_without_signal);
    RUN_TEST(setMined_side_chain_block_keeps_dah_when_unmined_since_nil);
    RUN_TEST(setMined_stale_higher_dah_unchanged);
    RUN_TEST(setMined_unset_last_block_clears_existing_dah);
    RUN_TEST(setMined_retention_zero_sets_no_dah);
    RUN_TEST(setMined_preserve_until_sets_no_dah);
    RUN_TEST(setMined_conflicting_dah_uses_block_height);
    RUN_TEST(setMined_mine_on_longest_chain_clears_unmined_since_and_sets_dah);

    RUN_TEST(setConflicting_set_true);
    RUN_TEST(setConflicting_set_false);

    RUN_TEST(setLocked_lock);
    RUN_TEST(setLocked_clears_delete_at_height);

    RUN_TEST(preserveUntil_success);
    RUN_TEST(preserveUntil_with_external);

    RUN_TEST(incrementSpentExtraRecs_increment);
    RUN_TEST(incrementSpentExtraRecs_decrement);
    RUN_TEST(incrementSpentExtraRecs_no_total);
    RUN_TEST(incrementSpentExtraRecs_negative_result);
    RUN_TEST(incrementSpentExtraRecs_exceeds_total);

    RUN_TEST(setDeleteAtHeight_all_spent_master);
    RUN_TEST(setDeleteAtHeight_preserve_blocks);
    RUN_TEST(setDeleteAtHeight_child_record_signal);

    // Record existence tests
    RUN_TEST(setMined_tx_not_found);
    RUN_TEST(setConflicting_tx_not_found);
    RUN_TEST(setLocked_tx_not_found);
    RUN_TEST(preserveUntil_tx_not_found);
    RUN_TEST(incrementSpentExtraRecs_tx_not_found);
}

TEST(setMined_ownership_after_args_and_result_destroy)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    int64_t block_id = 55555;

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, block_id);  // blockID as integer
    as_arraylist_append_int64(args, 600);   // blockHeight
    as_arraylist_append_int64(args, 2);     // subtreeIdx
    as_arraylist_append_int64(args, 1000);  // currentBlockHeight
    as_arraylist_append_int64(args, 100);   // blockHeightRetention
    as_arraylist_append(args, (as_val*)as_boolean_new(true));   // onLongestChain
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // unsetMined = false

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);
    as_arraylist_destroy(args);

    // Verify record still holds a valid copy after args/result destruction
    as_val* block_ids_val = as_rec_get(rec, "blockIDs");
    ASSERT_NOT_NULL(block_ids_val);
    as_list* block_ids = as_list_fromval(block_ids_val);
    ASSERT_EQ(as_list_size(block_ids), 1);
    as_integer* stored = as_integer_fromval(as_list_get(block_ids, 0));
    ASSERT_EQ(as_integer_get(stored), block_id);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

void run_additional_state_management_tests(void)
{
    RUN_TEST(setMined_ownership_after_args_and_result_destroy);
}
