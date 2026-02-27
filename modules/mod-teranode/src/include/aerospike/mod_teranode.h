/*
 * mod_teranode.h
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
 * Native TERANODE module for UTXO management.
 */

#pragma once

#include <aerospike/as_module.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

//==========================================================
// Public API.
//

// Global module instance - declared in mod_teranode.c
extern as_module mod_teranode;

// Thread-safe access (following mod_lua pattern)
void mod_teranode_rdlock(void);
void mod_teranode_wrlock(void);
void mod_teranode_unlock(void);

#ifdef __cplusplus
}
#endif
