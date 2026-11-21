package wallet

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

const atlasAccountAddressPrefix = "atl"

// Wallet is a local signing identity. Its key material remains in the chosen
// Cosmos keyring or in memory; it is never sent to an Atlas node.
//
// A Wallet serializes its own submissions. Separate wallets can submit in
// parallel without risking account-sequence collisions.
type Wallet struct {
	keyring keyring.Keyring
	keyName string
	address sdk.AccAddress
	txMu    sync.Mutex
}

// Address returns the wallet's bech32 account address.
func (w *Wallet) Address() string {
	if w == nil {
		return ""
	}
	address, err := sdk.Bech32ifyAddressBytes(atlasAccountAddressPrefix, w.address)
	if err != nil {
		return ""
	}
	return address
}

// KeyName returns the name used to find this wallet in its keyring.
func (w *Wallet) KeyName() string {
	if w == nil {
		return ""
	}
	return w.keyName
}

// NewKeyringWallet opens an existing key from a Cosmos SDK keyring.
func (m *WalletManager) NewKeyringWallet(cfg KeyringWalletConfig) (*Wallet, error) {
	if strings.TrimSpace(cfg.KeyName) == "" {
		return nil, fmt.Errorf("key name is required")
	}
	if strings.TrimSpace(cfg.Backend) == "" {
		return nil, fmt.Errorf("keyring backend is required")
	}
	if strings.TrimSpace(cfg.HomeDir) == "" {
		return nil, fmt.Errorf("keyring home directory is required")
	}
	if m == nil {
		return nil, fmt.Errorf("wallet manager is required")
	}
	input := cfg.Input
	if input == nil {
		input = os.Stdin
	}
	kr, err := keyring.New(sdk.KeyringServiceName(), cfg.Backend, cfg.HomeDir, input, m.codec)
	if err != nil {
		return nil, fmt.Errorf("create keyring: %w", err)
	}
	return walletFromKeyring(kr, cfg.KeyName)
}

// NewMnemonicWallet creates an in-memory wallet from a BIP-39 mnemonic. It
// does not need atlasd or a keyring directory on the caller's machine.
func (m *WalletManager) NewMnemonicWallet(cfg MnemonicWalletConfig) (*Wallet, error) {
	if m == nil {
		return nil, fmt.Errorf("wallet manager is required")
	}
	if strings.TrimSpace(cfg.KeyName) == "" {
		return nil, fmt.Errorf("key name is required")
	}
	if strings.TrimSpace(cfg.Mnemonic) == "" {
		return nil, fmt.Errorf("mnemonic is required")
	}
	if cfg.HDPath == "" {
		cfg.HDPath = sdk.FullFundraiserPath
	}
	kr := keyring.NewInMemory(m.codec)
	if _, err := kr.NewAccount(cfg.KeyName, cfg.Mnemonic, cfg.BIP39Passphrase, cfg.HDPath, hd.Secp256k1); err != nil {
		return nil, fmt.Errorf("import mnemonic wallet: %w", err)
	}
	return walletFromKeyring(kr, cfg.KeyName)
}

// NewPrivateKeyWallet creates an in-memory wallet from a raw hexadecimal
// secp256k1 private key. It accepts either bare hexadecimal or a 0x prefix.
func (m *WalletManager) NewPrivateKeyWallet(cfg PrivateKeyWalletConfig) (*Wallet, error) {
	if m == nil {
		return nil, fmt.Errorf("wallet manager is required")
	}
	if strings.TrimSpace(cfg.KeyName) == "" {
		return nil, fmt.Errorf("key name is required")
	}
	privateKey := strings.TrimSpace(cfg.PrivateKeyHex)
	if len(privateKey) < 2 {
		return nil, fmt.Errorf("private key is required")
	}
	kr := keyring.NewInMemory(m.codec)
	if err := kr.ImportPrivKeyHex(cfg.KeyName, privateKey, "secp256k1"); err != nil {
		return nil, fmt.Errorf("import private key wallet: %w", err)
	}
	return walletFromKeyring(kr, cfg.KeyName)
}

// walletFromKeyring loads the signing address stored under keyName.
func walletFromKeyring(kr keyring.Keyring, keyName string) (*Wallet, error) {
	info, err := kr.Key(keyName)
	if err != nil {
		return nil, fmt.Errorf("load key %q: %w", keyName, err)
	}
	address, err := info.GetAddress()
	if err != nil {
		return nil, fmt.Errorf("read address for key %q: %w", keyName, err)
	}
	return &Wallet{keyring: kr, keyName: keyName, address: address}, nil
}
