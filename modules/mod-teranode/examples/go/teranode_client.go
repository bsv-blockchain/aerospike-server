package teranode

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// ClientWrapper provides type-safe access to Teranode UDFs
type ClientWrapper struct {
	client *as.Client
}

// NewClientWrapper creates a new wrapper instance
func NewClientWrapper(client *as.Client) *ClientWrapper {
	return &ClientWrapper{client: client}
}

// execute is a helper to run the UDF
func (w *ClientWrapper) execute(policy *as.WritePolicy, key *as.Key, functionName string, args ...as.Value) (map[interface{}]interface{}, error) {
	res, err := w.client.Execute(policy, key, "teranode", functionName, args...)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return res.(map[interface{}]interface{}), nil
}

// Spend marks a single UTXO as spent
func (w *ClientWrapper) Spend(policy *as.WritePolicy, key *as.Key, offset int64, utxoHash []byte, spendingData []byte, ignoreConflicting bool, ignoreLocked bool, currentBlockHeight int64, blockHeightRetention int64) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "spend",
		as.NewLongValue(offset),
		as.NewBytesValue(utxoHash),
		as.NewBytesValue(spendingData),
		as.NewValue(ignoreConflicting),
		as.NewValue(ignoreLocked),
		as.NewLongValue(currentBlockHeight),
		as.NewLongValue(blockHeightRetention),
	)
}

// SpendMulti marks multiple UTXOs as spent in one operation
func (w *ClientWrapper) SpendMulti(policy *as.WritePolicy, key *as.Key, spends []map[string]interface{}, ignoreConflicting bool, ignoreLocked bool, currentBlockHeight int64, blockHeightRetention int64) (map[interface{}]interface{}, error) {
	// Convert Go map to generic interface map for Aerospike
	spendList := make([]interface{}, len(spends))
	for i, s := range spends {
		spendList[i] = s
	}

	return w.execute(policy, key, "spendMulti",
		as.NewListValue(spendList),
		as.NewValue(ignoreConflicting),
		as.NewValue(ignoreLocked),
		as.NewLongValue(currentBlockHeight),
		as.NewLongValue(blockHeightRetention),
	)
}

// Unspend reverses a spend operation
func (w *ClientWrapper) Unspend(policy *as.WritePolicy, key *as.Key, offset int64, utxoHash []byte, currentBlockHeight int64, blockHeightRetention int64) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "unspend",
		as.NewLongValue(offset),
		as.NewBytesValue(utxoHash),
		as.NewLongValue(currentBlockHeight),
		as.NewLongValue(blockHeightRetention),
	)
}

// Freeze prevents a UTXO from being spent
func (w *ClientWrapper) Freeze(policy *as.WritePolicy, key *as.Key, offset int64, utxoHash []byte) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "freeze",
		as.NewLongValue(offset),
		as.NewBytesValue(utxoHash),
	)
}

// Unfreeze allows a previously frozen UTXO to be spent
func (w *ClientWrapper) Unfreeze(policy *as.WritePolicy, key *as.Key, offset int64, utxoHash []byte) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "unfreeze",
		as.NewLongValue(offset),
		as.NewBytesValue(utxoHash),
	)
}

// Reassign changes a UTXO hash (used for frozen UTXOs)
func (w *ClientWrapper) Reassign(policy *as.WritePolicy, key *as.Key, offset int64, utxoHash []byte, newUtxoHash []byte, blockHeight int64, spendableAfter int64) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "reassign",
		as.NewLongValue(offset),
		as.NewBytesValue(utxoHash),
		as.NewBytesValue(newUtxoHash),
		as.NewLongValue(blockHeight),
		as.NewLongValue(spendableAfter),
	)
}

// SetMined tracks block height and ID for a transaction
func (w *ClientWrapper) SetMined(policy *as.WritePolicy, key *as.Key, blockID []byte, blockHeight int64, subtreeIdx int64, currentBlockHeight int64, blockHeightRetention int64, onLongestChain bool, unsetMined bool) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "setMined",
		as.NewBytesValue(blockID),
		as.NewLongValue(blockHeight),
		as.NewLongValue(subtreeIdx),
		as.NewLongValue(currentBlockHeight),
		as.NewLongValue(blockHeightRetention),
		as.NewValue(onLongestChain),
		as.NewValue(unsetMined),
	)
}

// SetConflicting marks/unmarks a transaction as conflicting
func (w *ClientWrapper) SetConflicting(policy *as.WritePolicy, key *as.Key, setValue bool, currentBlockHeight int64, blockHeightRetention int64) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "setConflicting",
		as.NewValue(setValue),
		as.NewLongValue(currentBlockHeight),
		as.NewLongValue(blockHeightRetention),
	)
}

// SetLocked locks/unlocks a transaction
func (w *ClientWrapper) SetLocked(policy *as.WritePolicy, key *as.Key, setValue bool) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "setLocked",
		as.NewValue(setValue),
	)
}

// PreserveUntil prevents record deletion until specific block height
func (w *ClientWrapper) PreserveUntil(policy *as.WritePolicy, key *as.Key, blockHeight int64) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "preserveUntil",
		as.NewLongValue(blockHeight),
	)
}

// IncrementSpentExtraRecs updates spent record counters for pagination
func (w *ClientWrapper) IncrementSpentExtraRecs(policy *as.WritePolicy, key *as.Key, inc int64, currentBlockHeight int64, blockHeightRetention int64) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "incrementSpentExtraRecs",
		as.NewLongValue(inc),
		as.NewLongValue(currentBlockHeight),
		as.NewLongValue(blockHeightRetention),
	)
}

// SetDeleteAtHeight manages record expiration (internal logic)
func (w *ClientWrapper) SetDeleteAtHeight(policy *as.WritePolicy, key *as.Key, currentBlockHeight int64, blockHeightRetention int64) (map[interface{}]interface{}, error) {
	return w.execute(policy, key, "setDeleteAtHeight",
		as.NewLongValue(currentBlockHeight),
		as.NewLongValue(blockHeightRetention),
	)
}
