package wallet

import (
	"errors"
	"io"
	"sync"
	"time"
)

// ErrWalletManagerClosed reports that a manager can no longer accept work.
var ErrWalletManagerClosed = errors.New("wallet manager is closed")

// WalletManagerConfig configures a connection used to sign and submit Atlas
// transactions. A gRPC endpoint is required because it provides account
// sequence queries, gas simulation, and the Cosmos transaction service.
type WalletManagerConfig struct {
	ChainID     string
	GRPCAddress string

	// GRPCInsecure disables TLS. Use it only for a local development chain.
	GRPCInsecure bool

	// GasPrices is a Cosmos decimal coin string, for example "0.025uatl".
	// It defaults to the Atlas network minimum used by the SDK.
	GasPrices string
	// GasAdjustment is applied to simulated gas. It defaults to 1.2.
	GasAdjustment float64
	// GasLimit skips simulation when non-zero.
	GasLimit uint64

	// PollInterval controls WaitForTx polling. It defaults to one second.
	PollInterval time.Duration
	// QueueCapacity is the number of pending broadcasts allowed per wallet. It
	// defaults to 32; set it to -1 for an unbounded in-memory queue.
	QueueCapacity int
}

// KeyringWalletConfig selects an existing key in a Cosmos SDK keyring.
// Input is used only by keyring backends that prompt for a passphrase.
type KeyringWalletConfig struct {
	KeyName string
	Backend string
	HomeDir string
	Input   io.Reader
}

// MnemonicWalletConfig creates a transient, in-memory wallet from a BIP-39
// mnemonic. The mnemonic is never written by this SDK.
type MnemonicWalletConfig struct {
	KeyName         string
	Mnemonic        string
	BIP39Passphrase string
	// HDPath defaults to Cosmos's standard BIP-44 path.
	HDPath string
}

// PrivateKeyWalletConfig creates a transient, in-memory wallet from a raw
// secp256k1 private key. PrivateKeyHex may include a 0x prefix and is never
// written by this SDK.
type PrivateKeyWalletConfig struct {
	KeyName       string
	PrivateKeyHex string
}

// BroadcastOptions applies to one transaction. Zero-valued fields use the
// manager defaults.
type BroadcastOptions struct {
	// GasPrices overrides WalletManagerConfig.GasPrices for this transaction.
	GasPrices string
	// GasLimit skips gas simulation for this transaction when non-zero.
	GasLimit uint64
	Memo     string
}

// TxResult is the node's CheckTx result, or the DeliverTx result after
// WaitForTx. A non-zero Code indicates the chain rejected the transaction.
type TxResult struct {
	Hash   string
	Height int64
	Code   uint32
	Log    string
}

// TxFuture represents one asynchronous transaction submission.
// Result waits for CheckTx; Wait waits for block inclusion after CheckTx.
type TxFuture struct {
	manager *WalletManager
	done    chan struct{}

	resultOnce sync.Once
	resultMu   sync.RWMutex
	result     *TxResult
	err        error
}
