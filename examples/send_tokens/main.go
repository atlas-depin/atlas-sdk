// Command send_tokens sends uatl from a mnemonic or private-key wallet.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/atlas-depin/atlas-sdk/go/wallet"
	sdk "github.com/cosmos/cosmos-sdk/types"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
)

// main runs the token-transfer example and reports any failure to stderr.
func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// run loads configuration, broadcasts MsgSend, and waits for its commitment.
func run(parent context.Context) error {
	chainID, err := requiredEnv("ATLAS_CHAIN_ID")
	if err != nil {
		return err
	}
	grpcAddress, err := requiredEnv("ATLAS_GRPC_ADDRESS")
	if err != nil {
		return err
	}
	recipient, err := requiredEnv("ATLAS_RECIPIENT")
	if err != nil {
		return err
	}
	amountText, err := requiredEnv("ATLAS_AMOUNT")
	if err != nil {
		return err
	}
	amount, err := sdk.ParseCoinNormalized(amountText)
	if err != nil || !amount.IsValid() || !amount.IsPositive() {
		return fmt.Errorf("ATLAS_AMOUNT must be a positive Cosmos coin, such as 1000000uatl")
	}

	insecure, err := strconv.ParseBool(strings.TrimSpace(os.Getenv("ATLAS_GRPC_INSECURE")))
	if strings.TrimSpace(os.Getenv("ATLAS_GRPC_INSECURE")) == "" {
		insecure = false
		err = nil
	}
	if err != nil {
		return fmt.Errorf("parse ATLAS_GRPC_INSECURE: %w", err)
	}
	connectCtx, cancelConnect := context.WithTimeout(parent, 20*time.Second)
	defer cancelConnect()
	manager, err := wallet.NewWalletManager(connectCtx, wallet.WalletManagerConfig{
		ChainID:      chainID,
		GRPCAddress:  grpcAddress,
		GRPCInsecure: insecure,
		GasPrices:    strings.TrimSpace(os.Getenv("ATLAS_GAS_PRICES")),
	})
	if err != nil {
		return err
	}
	defer manager.Close()

	sender, err := walletFromEnvironment(manager)
	if err != nil {
		return err
	}
	message := &banktypes.MsgSend{
		FromAddress: sender.Address(),
		ToAddress:   recipient,
		Amount:      sdk.NewCoins(amount),
	}
	broadcastCtx, cancelBroadcast := context.WithTimeout(parent, 30*time.Second)
	defer cancelBroadcast()
	checked, err := manager.BroadcastTx(broadcastCtx, sender, message)
	if err != nil {
		return fmt.Errorf("broadcast token transfer: %w", err)
	}

	commitCtx, cancelCommit := context.WithTimeout(parent, 2*time.Minute)
	defer cancelCommit()
	committed, err := manager.WaitForTx(commitCtx, checked.Hash)
	if err != nil {
		return fmt.Errorf("wait for token transfer %s: %w", checked.Hash, err)
	}
	fmt.Printf("sent %s to %s in transaction %s at height %d\n", amount, recipient, committed.Hash, committed.Height)
	return nil
}

// walletFromEnvironment creates a transient sender from exactly one secret.
func walletFromEnvironment(manager *wallet.WalletManager) (*wallet.Wallet, error) {
	keyName := strings.TrimSpace(os.Getenv("ATLAS_KEY_NAME"))
	if keyName == "" {
		keyName = "send-tokens"
	}
	mnemonic := strings.TrimSpace(os.Getenv("ATLAS_MNEMONIC"))
	privateKey := strings.TrimSpace(os.Getenv("ATLAS_PRIVATE_KEY"))
	if mnemonic != "" && privateKey != "" {
		return nil, fmt.Errorf("set only one of ATLAS_MNEMONIC or ATLAS_PRIVATE_KEY")
	}
	if mnemonic != "" {
		return manager.NewMnemonicWallet(wallet.MnemonicWalletConfig{KeyName: keyName, Mnemonic: mnemonic})
	}
	if privateKey != "" {
		return manager.NewPrivateKeyWallet(wallet.PrivateKeyWalletConfig{KeyName: keyName, PrivateKeyHex: privateKey})
	}
	return nil, fmt.Errorf("set ATLAS_MNEMONIC or ATLAS_PRIVATE_KEY")
}

// requiredEnv returns a non-empty environment variable or a useful error.
func requiredEnv(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
