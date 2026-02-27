# TERANODE Native Module for Aerospike

Native C implementation of UTXO management functions for high-performance blockchain operations.

## Features

- **Native Performance**: Replaces Lua-based logic with optimized C code
- **Complete Feature Set**: Implements all 12 UTXO management functions
- **Thread Safe**: Uses reader-writer locks for concurrent access protection
- **Drop-in Replacement**: Maintains exact functional parity with existing Lua interface

## Functions

The module exports the following functions for use in UDF calls:

1. **spend** - Mark a single UTXO as spent
2. **spendMulti** - Mark multiple UTXOs as spent in one operation
3. **unspend** - Reverse a spend operation
4. **freeze** - Prevent a UTXO from being spent
5. **unfreeze** - Allow a previously frozen UTXO to be spent
6. **reassign** - Change a UTXO hash (used for frozen UTXOs)
7. **setMined** - Track block height and ID for a transaction
8. **setConflicting** - Mark/unmark a transaction as conflicting
9. **setLocked** - Lock/unlock a transaction
10. **preserveUntil** - Prevent record deletion until specific block height
11. **incrementSpentExtraRecs** - Update spent record counters for pagination
12. **setDeleteAtHeight** - Internal logic to manage record expiration

## Usage

Functions are called via the Aerospike UDF interface. You can use the raw client directly or use the provided helper wrapper.

### Go Wrapper (Recommended)

A type-safe Go wrapper is available in `examples/go/`.

```go
import "teranode-client"

wrapper := teranode.NewClientWrapper(client)
res, err := wrapper.Spend(policy, key, offset, utxoHash, spendingData, false, false, height, retention)
```

### Raw Client Examples

Below are examples using the Aerospike Go client (v8) directly.

### 1. spend
```go
args := []as.Value{
    as.NewLongValue(0),                  // offset
    as.NewBytesValue(utxoHash),          // utxoHash (32 bytes)
    as.NewBytesValue(spendingData),      // spendingData (36 bytes)
    as.NewValue(false),                  // ignoreConflicting
    as.NewValue(false),                  // ignoreLocked
    as.NewLongValue(800000),             // currentBlockHeight
    as.NewLongValue(1000),               // blockHeightRetention
}
res, err := client.Execute(policy, key, "teranode", "spend", args...)
```

### 2. spendMulti
```go
spendMap := map[interface{}]interface{}{
    "offset": 0,
    "utxoHash": utxoHash,
    "spendingData": spendingData,
    "idx": 1,
}
spends := []interface{}{spendMap}

args := []as.Value{
    as.NewListValue(spends),             // spends
    as.NewValue(false),                  // ignoreConflicting
    as.NewValue(false),                  // ignoreLocked
    as.NewLongValue(800000),             // currentBlockHeight
    as.NewLongValue(1000),               // blockHeightRetention
}
res, err := client.Execute(policy, key, "teranode", "spendMulti", args...)
```

### 3. unspend
```go
args := []as.Value{
    as.NewLongValue(0),                  // offset
    as.NewBytesValue(utxoHash),          // utxoHash
    as.NewLongValue(800000),             // currentBlockHeight
    as.NewLongValue(1000),               // blockHeightRetention
}
res, err := client.Execute(policy, key, "teranode", "unspend", args...)
```

### 4. freeze
```go
args := []as.Value{
    as.NewLongValue(0),                  // offset
    as.NewBytesValue(utxoHash),          // utxoHash
}
res, err := client.Execute(policy, key, "teranode", "freeze", args...)
```

### 5. unfreeze
```go
args := []as.Value{
    as.NewLongValue(0),                  // offset
    as.NewBytesValue(utxoHash),          // utxoHash
}
res, err := client.Execute(policy, key, "teranode", "unfreeze", args...)
```

### 6. reassign
```go
args := []as.Value{
    as.NewLongValue(0),                  // offset
    as.NewBytesValue(oldHash),           // utxoHash
    as.NewBytesValue(newHash),           // newUtxoHash
    as.NewLongValue(800000),             // blockHeight
    as.NewLongValue(800006),             // spendableAfter
}
res, err := client.Execute(policy, key, "teranode", "reassign", args...)
```

### 7. setMined
```go
args := []as.Value{
    as.NewBytesValue(blockID),           // blockID
    as.NewLongValue(800000),             // blockHeight
    as.NewLongValue(0),                  // subtreeIdx
    as.NewLongValue(800000),             // currentBlockHeight
    as.NewLongValue(1000),               // blockHeightRetention
    as.NewValue(true),                   // onLongestChain
    as.NewValue(false),                  // unsetMined
}
res, err := client.Execute(policy, key, "teranode", "setMined", args...)
```

### 8. setConflicting
```go
args := []as.Value{
    as.NewValue(true),                   // setValue (true=set, false=clear)
    as.NewLongValue(800000),             // currentBlockHeight
    as.NewLongValue(1000),               // blockHeightRetention
}
res, err := client.Execute(policy, key, "teranode", "setConflicting", args...)
```

### 9. setLocked
```go
args := []as.Value{
    as.NewValue(true),                   // setValue (true=lock, false=unlock)
}
res, err := client.Execute(policy, key, "teranode", "setLocked", args...)
```

### 10. preserveUntil
```go
args := []as.Value{
    as.NewLongValue(801000),             // blockHeight
}
res, err := client.Execute(policy, key, "teranode", "preserveUntil", args...)
```

### 11. incrementSpentExtraRecs
```go
args := []as.Value{
    as.NewLongValue(1),                  // inc (can be negative)
    as.NewLongValue(800000),             // currentBlockHeight
    as.NewLongValue(1000),               // blockHeightRetention
}
res, err := client.Execute(policy, key, "teranode", "incrementSpentExtraRecs", args...)
```

### 12. setDeleteAtHeight
```go
args := []as.Value{
    as.NewLongValue(800000),             // currentBlockHeight
    as.NewLongValue(1000),               // blockHeightRetention
}
res, err := client.Execute(policy, key, "teranode", "setDeleteAtHeight", args...)
```

## Building

The module can be built independently or as part of the server build.

### Standalone Build

```bash
cd modules/mod-teranode
make
```
This produces `target/lib/libmod_teranode.dylib` (or `.so` on Linux).

### Integration Build

When building the full Aerospike server, the module is built automatically if included in the build configuration.

```bash
# From server root
make
```

## Testing

The module includes a comprehensive C-based test suite that mocks Aerospike server structures.

```bash
cd modules/mod-teranode
make test
```

## File Structure

### Source (`src/main/`)
- `mod_teranode.c`: Module entry point, lifecycle hooks, and function dispatch
- `mod_teranode_utxo.c`: Core logic implementation for all UTXO functions
- `internal.h`: Internal shared definitions

### Headers (`src/include/aerospike/`)
- `mod_teranode.h`: Public module interface
- `mod_teranode_utxo.h`: UTXO function declarations and constants
- `mod_teranode_config.h`: Configuration structures

### Tests (`src/test/`)
- `test_main.c`: Test runner
- `test_spend.c`: Tests for spend/unspend logic
- `test_freeze.c`: Tests for freeze/unfreeze/reassign
- `test_state_management.c`: Tests for locking, mining, and retention logic
- `mock_record.c`: Mocks for Aerospike records (bins, maps, lists)

## License

Copyright (C) 2026 BSV Association.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Affero General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
