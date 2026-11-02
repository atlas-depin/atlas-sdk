package atlas

import (
	"fmt"
	"os"
	"strings"

	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

// Wallet is a local Cosmos SDK keyring identity. It never sends private key
// material to the network.
type Wallet struct {
	keyring keyring.Keyring
	keyName string
	address sdk.AccAddress
}

// NewKeyringWallet opens an existing key from a Cosmos SDK keyring.
func (c *Client) NewKeyringWallet(cfg WalletConfig) (*Wallet, error) {
	if strings.TrimSpace(cfg.KeyName) == "" {
		return nil, fmt.Errorf("key name is required")
	}
	if strings.TrimSpace(cfg.Backend) == "" {
		return nil, fmt.Errorf("keyring backend is required")
	}
	if strings.TrimSpace(cfg.HomeDir) == "" {
		return nil, fmt.Errorf("keyring home directory is required")
	}
	input := cfg.Input
	if input == nil {
		input = os.Stdin
	}
	kr, err := keyring.New(sdk.KeyringServiceName(), cfg.Backend, cfg.HomeDir, input, c.codec)
	if err != nil {
		return nil, fmt.Errorf("create keyring: %w", err)
	}
	info, err := kr.Key(cfg.KeyName)
	if err != nil {
		return nil, fmt.Errorf("load key %q: %w", cfg.KeyName, err)
	}
	address, err := info.GetAddress()
	if err != nil {
		return nil, fmt.Errorf("read address for key %q: %w", cfg.KeyName, err)
	}
	return &Wallet{keyring: kr, keyName: cfg.KeyName, address: address}, nil
}

// Address returns the wallet's bech32 account address.
func (w *Wallet) Address() string {
	if w == nil {
		return ""
	}
	return w.address.String()
}

// KeyName returns the keyring name used for signing.
func (w *Wallet) KeyName() string {
	if w == nil {
		return ""
	}
	return w.keyName
}
