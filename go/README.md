# Atlas Go SDK

The SDK is a Go client for Atlas DePIN storage and filetree operations.

## Transport policy

`TransportAuto` is the default and preferred configuration. It uses gRPC for
Atlas's typed module queries and Cosmos transaction services. Set
`TransportRPC` to broadcast and poll transactions over CometBFT JSON-RPC while
still configuring `GRPCAddress` for typed module queries and transaction
signing.

The current Atlas protocol publishes `storage` and `filetree` queries as gRPC
services; CometBFT JSON-RPC alone cannot serve those typed APIs.

## Provider transfers

An upload computes its commitment from 1 KiB chunks, stages `MsgPostFile`,
waits for the transaction to commit, and streams a multipart upload to
`/api/v1/upload` on registered providers. Downloads use `/api/v1/download/:id`
and write only after the response verifies against the on-chain Merkle root.

The Merkle format follows the current provider implementation: BLAKE3 chunk
hashes followed by an XXH128 Merkle tree. This deliberately differs from the
older ClipSync relay example.
