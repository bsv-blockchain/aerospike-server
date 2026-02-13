/*
 * mod_teranode.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Native TERANODE module implementation.
 * Implements as_module_hooks interface for UTXO management.
 */

#include <aerospike/mod_teranode.h>
#include <aerospike/mod_teranode_config.h>
#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_module.h>
#include <aerospike/as_result.h>
#include <aerospike/as_udf_context.h>
#include <aerospike/as_val.h>
#include <aerospike/as_list.h>
#include <aerospike/as_string.h>

#include <pthread.h>
#include <string.h>
#include <stdbool.h>

#include "internal.h"

//==========================================================
// Typedefs.
//

// Function pointer type for UTXO functions
typedef as_val* (*teranode_fn)(as_rec*, as_list*, as_aerospike*);

//==========================================================
// Forward declarations.
//

static int teranode_update(as_module* m, as_module_event* e);
static int teranode_validate(as_module* m, as_aerospike* as,
    const char* filename, const char* content, uint32_t size,
    as_module_error* err);
static int teranode_apply_record(as_module* m, as_udf_context* ctx,
    const char* filename, const char* function,
    as_rec* rec, as_list* args, as_result* res);

//==========================================================
// Globals.
//

// Module configuration
static mod_teranode_config g_config;

// Read-write lock for thread safety
static pthread_rwlock_t g_lock = PTHREAD_RWLOCK_INITIALIZER;


// Module hooks
static const as_module_hooks mod_teranode_hooks = {
    .destroy        = NULL,
    .update         = teranode_update,
    .validate       = teranode_validate,
    .apply_record   = teranode_apply_record,
    .apply_stream   = NULL  // Stream operations not supported
};

// Global module instance
as_module mod_teranode = {
    .source = NULL,
    .hooks  = &mod_teranode_hooks
};

//==========================================================
// Public API - Locking.
//
// These mirror the mod_lua locking interface. A read lock is acquired
// during UDF execution; a write lock is acquired when the module
// configuration is updated.
//

/** Acquire a shared read lock on the module state. */
void
mod_teranode_rdlock(void)
{
    pthread_rwlock_rdlock(&g_lock);
}

/** Acquire an exclusive write lock on the module state. */
void
mod_teranode_wrlock(void)
{
    pthread_rwlock_wrlock(&g_lock);
}

/** Release the module state lock (read or write). */
void
mod_teranode_unlock(void)
{
    pthread_rwlock_unlock(&g_lock);
}

//==========================================================
// Module hooks implementation.
//

/**
 * Handle module lifecycle events.
 */
static int
teranode_update(as_module* m, as_module_event* e)
{
    (void)m;

    switch (e->type) {
    case AS_MODULE_EVENT_CONFIGURE:
        if (e->data.config != NULL) {
            memcpy(&g_config, e->data.config, sizeof(mod_teranode_config));
            LOG_INFO("mod-teranode configured: enabled=%d", g_config.enabled);
        }
        break;

    case AS_MODULE_EVENT_FILE_SCAN:
    case AS_MODULE_EVENT_FILE_ADD:
    case AS_MODULE_EVENT_FILE_REMOVE:
    case AS_MODULE_EVENT_CLEAR_CACHE:
        // No-op for native module - no files to manage
        break;

    default:
        LOG_WARN("mod-teranode: unknown event type %d", e->type);
        break;
    }

    return 0;
}

/**
 * Validate module content.
 * For native modules, always succeeds (no code to validate).
 */
static int
teranode_validate(as_module* m, as_aerospike* as,
    const char* filename, const char* content, uint32_t size,
    as_module_error* err)
{
    (void)m;
    (void)as;
    (void)filename;
    (void)content;
    (void)size;
    (void)err;

    // Native module - nothing to validate
    return 0;
}

/**
 * Apply a named function to a record — the main entry point for UDF execution.
 *
 * Resolves the function name to a teranode_fn pointer using first-character
 * dispatch (switch on function[0], then strcmp within each group). This
 * reduces average strcmp calls from ~6 to ~2 for the 12 registered functions.
 *
 * Groups: 's' (spend, spendMulti, setMined, setConflicting, setLocked,
 * setDeleteAtHeight), 'u' (unspend, unfreeze), 'f' (freeze),
 * 'i' (incrementSpentExtraRecs), 'p' (preserveUntil), 'r' (reassign).
 *
 * After execution, decrements the record refcount to compensate for the
 * unconditional as_val_reserve in udf.c (see comment at label 'done').
 */
static int
teranode_apply_record(as_module* m, as_udf_context* ctx,
    const char* filename, const char* function,
    as_rec* rec, as_list* args, as_result* res)
{
    (void)m;
    (void)filename;

    int rc = -1;

    if (function == NULL) {
        LOG_ERROR("mod-teranode: function name is NULL");
        as_result_setfailure(res, (as_val*)as_string_new("function name required", false));
        goto done;
    }

    // Find function via first-character dispatch.
    // Reduces average strcmp calls from ~6 to ~2 for 12 entries.
    teranode_fn fn = NULL;

    switch (function[0]) {
    case 's':
        // spend, spendMulti, setMined, setConflicting, setLocked, setDeleteAtHeight
        if (strcmp(function, "spend") == 0) fn = teranode_spend;
        else if (strcmp(function, "spendMulti") == 0) fn = teranode_spend_multi;
        else if (strcmp(function, "setMined") == 0) fn = teranode_set_mined;
        else if (strcmp(function, "setConflicting") == 0) fn = teranode_set_conflicting;
        else if (strcmp(function, "setLocked") == 0) fn = teranode_set_locked;
        else if (strcmp(function, "setDeleteAtHeight") == 0) fn = teranode_set_delete_at_height;
        break;
    case 'u':
        // unspend, unfreeze
        if (strcmp(function, "unspend") == 0) fn = teranode_unspend;
        else if (strcmp(function, "unfreeze") == 0) fn = teranode_unfreeze;
        break;
    case 'f':
        if (strcmp(function, "freeze") == 0) fn = teranode_freeze;
        break;
    case 'i':
        if (strcmp(function, "incrementSpentExtraRecs") == 0) fn = teranode_increment_spent_extra_recs;
        break;
    case 'p':
        if (strcmp(function, "preserveUntil") == 0) fn = teranode_preserve_until;
        break;
    case 'r':
        if (strcmp(function, "reassign") == 0) fn = teranode_reassign;
        break;
    default:
        break;
    }

    if (fn == NULL) {
        LOG_ERROR("mod-teranode: unknown function '%s'", function);
        char errmsg[256];
        snprintf(errmsg, sizeof(errmsg), "unknown function: %s", function);
        char* msg_copy = strdup(errmsg);
        if (msg_copy != NULL) {
            as_string* s = as_string_new(msg_copy, true);
            if (s != NULL) {
                as_result_setfailure(res, (as_val*)s);
            } else {
                free(msg_copy);
                as_result_setfailure(res, (as_val*)as_string_new("unknown function", false));
            }
        } else {
            as_result_setfailure(res, (as_val*)as_string_new("unknown function", false));
        }
        goto done;
    }

    // Execute the function - pass ctx->as so functions can call aerospike:update(rec)
    // like the Lua script does, committing changes inside each function on success
    as_aerospike* as_ctx = (ctx != NULL) ? ctx->as : NULL;
    as_val* result = fn(rec, args, as_ctx);

    if (result == NULL) {
        LOG_ERROR("mod-teranode: function '%s' returned NULL", function);
        as_result_setfailure(res, (as_val*)as_string_new("function returned NULL", false));
        goto done;
    }

    // Set success result
    // Note: Record updates are now handled inside each function (like Lua script)
    as_result_setsuccess(res, result);
    rc = 0;

done:
    // Compensate for the unconditional as_val_reserve(urec) in udf.c
    // (transaction/udf.c:900). That reserve exists for Lua's garbage collector
    // which calls as_val_destroy via __gc when it collects the record userdata.
    // Native modules have no GC, so without this the as_rec leaks on every call.
    //
    // The proper fix is to make the reserve in udf.c conditional on the module
    // type. This workaround is safe: it decrements the refcount from 2 to 1 here,
    // then udf_master_done/failed decrements from 1 to 0 and frees.
    as_val_destroy((as_val*)rec);

    return rc;
}
