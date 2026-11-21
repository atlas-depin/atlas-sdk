package wallet

import (
	"strings"
	"testing"

	filetreetypes "github.com/atlas-depin/atlas/x/filetree/types"
	storagetypes "github.com/atlas-depin/atlas/x/storage/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	distributiontypes "github.com/cosmos/cosmos-sdk/x/distribution/types"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types/v1"
	stakingtypes "github.com/cosmos/cosmos-sdk/x/staking/types"
	transfertypes "github.com/cosmos/ibc-go/v10/modules/apps/transfer/types"
)

const testMnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"

// TestNewMnemonicWalletCreatesInMemorySigner verifies mnemonic import.
func TestNewMnemonicWalletCreatesInMemorySigner(t *testing.T) {
	manager := testWalletManager()
	wallet, err := manager.NewMnemonicWallet(MnemonicWalletConfig{
		KeyName:  "primary",
		Mnemonic: testMnemonic,
	})
	if err != nil {
		t.Fatalf("NewMnemonicWallet() error = %v", err)
	}
	if !strings.HasPrefix(wallet.Address(), "atl1") {
		t.Fatalf("wallet address = %q, want Atlas bech32 prefix", wallet.Address())
	}
	if wallet.KeyName() != "primary" {
		t.Fatalf("KeyName() = %q, want primary", wallet.KeyName())
	}
}

// TestNewPrivateKeyWalletCreatesInMemorySigner verifies raw-key import.
func TestNewPrivateKeyWalletCreatesInMemorySigner(t *testing.T) {
	manager := testWalletManager()
	wallet, err := manager.NewPrivateKeyWallet(PrivateKeyWalletConfig{
		KeyName:       "primary",
		PrivateKeyHex: "0x0000000000000000000000000000000000000000000000000000000000000001",
	})
	if err != nil {
		t.Fatalf("NewPrivateKeyWallet() error = %v", err)
	}
	if !strings.HasPrefix(wallet.Address(), "atl1") {
		t.Fatalf("wallet address = %q, want Atlas bech32 prefix", wallet.Address())
	}
}

// TestWalletImportRejectsMissingSecrets verifies import validation.
func TestWalletImportRejectsMissingSecrets(t *testing.T) {
	manager := testWalletManager()
	if _, err := manager.NewMnemonicWallet(MnemonicWalletConfig{KeyName: "primary"}); err == nil {
		t.Fatal("NewMnemonicWallet() error = nil, want missing mnemonic error")
	}
	if _, err := manager.NewPrivateKeyWallet(PrivateKeyWalletConfig{KeyName: "primary"}); err == nil {
		t.Fatal("NewPrivateKeyWallet() error = nil, want missing private key error")
	}
}

// TestWalletManagerConfigValidation verifies configuration validation.
func TestWalletManagerConfigValidation(t *testing.T) {
	if _, err := NewWalletManager(t.Context(), WalletManagerConfig{}); err == nil {
		t.Fatal("NewWalletManager() error = nil, want missing chain ID error")
	}
	if _, err := NewWalletManager(t.Context(), WalletManagerConfig{ChainID: "atlas-1"}); err == nil {
		t.Fatal("NewWalletManager() error = nil, want missing gRPC address error")
	}
	if _, err := NewWalletManager(t.Context(), WalletManagerConfig{
		ChainID:     "atlas-1",
		GRPCAddress: "localhost:9090",
		GasPrices:   "not-a-coin",
	}); err == nil {
		t.Fatal("NewWalletManager() error = nil, want invalid gas price error")
	}
	if _, err := NewWalletManager(t.Context(), WalletManagerConfig{
		ChainID:       "atlas-1",
		GRPCAddress:   "localhost:9090",
		QueueCapacity: -2,
	}); err == nil {
		t.Fatal("NewWalletManager() error = nil, want invalid queue capacity error")
	}
}

// TestIsSequenceMismatch verifies retryable sequence detection.
func TestIsSequenceMismatch(t *testing.T) {
	if !isSequenceMismatch(&TxResult{Log: "account sequence mismatch, expected 9, got 8"}, nil) {
		t.Fatal("sequence mismatch was not recognized")
	}
	if isSequenceMismatch(&TxResult{Log: "insufficient funds"}, nil) {
		t.Fatal("non-sequence error was recognized as sequence mismatch")
	}
}

// TestEncodingConfigSupportsAtlasMessages verifies Atlas message encoding.
func TestEncodingConfigSupportsAtlasMessages(t *testing.T) {
	encoding := newEncodingConfig()
	builder := encoding.txConfig.NewTxBuilder()
	if err := builder.SetMsgs(
		&banktypes.MsgSend{FromAddress: "atl1test", ToAddress: "atl1recipient", Amount: sdk.NewCoins(sdk.NewInt64Coin("uatl", 1))},
		&stakingtypes.MsgDelegate{DelegatorAddress: "atl1test", ValidatorAddress: "atlvaloper1test", Amount: sdk.NewInt64Coin("uatl", 1)},
		&distributiontypes.MsgWithdrawDelegatorReward{DelegatorAddress: "atl1test", ValidatorAddress: "atlvaloper1test"},
		&govtypes.MsgVote{ProposalId: 1, Voter: "atl1test", Option: govtypes.OptionYes},
		&transfertypes.MsgTransfer{SourcePort: "transfer", SourceChannel: "channel-0", Token: sdk.NewInt64Coin("uatl", 1), Sender: "atl1test", Receiver: "cosmos1recipient"},
		&storagetypes.MsgBuyStorage{Creator: "atl1test", Duration: 1, Bytes: 1},
		&filetreetypes.MsgPostNode{Creator: "atl1test", Path: "/", NodeType: "folder"},
	); err != nil {
		t.Fatalf("SetMsgs() error = %v", err)
	}
	if _, err := encoding.txConfig.TxEncoder()(builder.GetTx()); err != nil {
		t.Fatalf("TxEncoder() error = %v", err)
	}
}

// testWalletManager creates the codec-only manager needed by local tests.
func testWalletManager() *WalletManager {
	encoding := newEncodingConfig()
	return &WalletManager{codec: encoding.codec}
}
