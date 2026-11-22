# Atlas SDK

Developer libraries for building applications using Atlas DePIN storage. 
The SDK is organized by language, with shared runnable examples in
`examples/`.

## Status

The Go SDK is the current implementation. It provides a wallet manager that
signs and broadcasts Atlas blockchain transactions. Storage workflows, chain
queries, and additional language SDKs will be added alongside it over time.

The `cpp/` directory is reserved for the future C++ SDK; it is not yet an
implemented library.

## Go wallet SDK

Import the wallet package:

```go
import "github.com/atlas-depin/atlas-sdk/go/wallet"
```

Supports Cosmos keyring wallets and transient mnemonic/private-key wallets;
the latter keeps the mnemonic in memory and does not require `atlasd`.

```go
manager, err := wallet.NewWalletManager(ctx, wallet.WalletManagerConfig{
	ChainID:     "itineraria-1",
	GRPCAddress: "grpc.atlasdepin.com:443",
})
if err != nil {
	return err
}
defer manager.Close()

sender, err := manager.NewMnemonicWallet(wallet.MnemonicWalletConfig{
	KeyName:  "primary",
	Mnemonic: mnemonic,
})
```

Use `BroadcastTx` for a synchronous CheckTx result, or `BroadcastTxAsync` for
a Golang future. Both calls enter the same FIFO queue for a wallet, preventing account
sequence conflicts when several workflows use it concurrently.

